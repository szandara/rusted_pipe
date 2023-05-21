use prometheus::core::GenericGauge;
use prometheus_exporter::Exporter;
use pyroscope::pyroscope::PyroscopeAgentRunning;
use pyroscope::PyroscopeAgent;
use pyroscope_pprofrs::{pprof_backend, PprofConfig};
use lazy_static::lazy_static;
use prometheus::{register_int_gauge_vec, IntGaugeVec};

lazy_static! {
    static ref SIZE_METRIC: IntGaugeVec = register_int_gauge_vec!(
        "queue_size", "Size of buffering queues",
        &["node_id", "channel_id"]
    )
    .expect("Cannot create queue_size metrics");
}

pub const MACOS_DOCKER_ADDRESS: &str = "host.docker.internal";
pub const LOCALHOST: &str = "127.0.0.1";

pub struct Profiler {
    pub profiler: PyroscopeAgent<PyroscopeAgentRunning>,
}

pub struct ProfilerTag {
    pub add_fn: Box<dyn Fn(String, String) -> pyroscope::Result<()>>,
    pub remove_fn: Box<dyn Fn(String, String) -> pyroscope::Result<()>>,
}

unsafe impl Send for ProfilerTag {}
unsafe impl Sync for ProfilerTag {}

impl ProfilerTag {
    pub fn from_tuple(
        (add, remove): (
            impl Fn(String, String) -> pyroscope::Result<()> + 'static,
            impl Fn(String, String) -> pyroscope::Result<()> + 'static,
        ),
    ) -> Self {
        ProfilerTag {
            add_fn: Box::new(add),
            remove_fn: Box::new(remove),
        }
    }

    pub fn add(&self, key: String, value: String) {
        let _ = (self.add_fn)(key, value);
    }

    pub fn remove(&self, key: String, value: String) {
        let _ = (self.remove_fn)(key, value);
    }

    pub fn no_profiler() -> Self {
        ProfilerTag {
            add_fn: Box::new(|_: String, _: String| Ok(())),
            remove_fn: Box::new(|_: String, _: String| Ok(())),
        }
    }
}

/// Creates a prometheus address on localhost:9001
pub fn default_prometheus_address() -> String {
    format!("{LOCALHOST}:9001")
}

/// Default pyroscope server address
pub fn default_pyroscope_address() -> String {
    format!("{LOCALHOST}:4040")
}

pub struct Metrics {
    metrics_server: Option<MetricsServer>,
    pyroscope_agent: Option<Profiler>,
}

impl Metrics {
    pub fn profiler(&self) -> &Option<Profiler> {
        &self.pyroscope_agent
    }

    pub fn metrics_server(&self) -> &Option<MetricsServer> {
        &self.metrics_server
    }

    pub fn stop(self) {
        if let Some(server) = self.metrics_server {
            server.stop()
        }
    }

    pub fn no_metrics() -> Self {
        Metrics {
            metrics_server: None,
            pyroscope_agent: None,
        }
    }

    pub fn builder() -> Self {
        Metrics {
            metrics_server: None,
            pyroscope_agent: None,
        }
    }

    pub fn with_pyroscope(self, pyroscope_server_addr: &str) -> Self {
        Metrics {
            metrics_server: self.metrics_server,
            pyroscope_agent: Some(create_profiler_agent(pyroscope_server_addr)),
        }
    }

    pub fn with_prometheus(self, prometheus_addr: &str) -> Self {
        Metrics {
            metrics_server: Some(spawn_metrics_server(prometheus_addr)),
            pyroscope_agent: self.pyroscope_agent,
        }
    }
}

pub struct MetricsServer {
    _exporter: Exporter,
}

impl MetricsServer {
    pub fn stop(self) {
        println!("Shut down Prometheus server");
    }
}

impl Profiler {
    pub fn stop(self) {
        let agent_ready = self.profiler.stop().expect("Cannot stop Pyroscope agent.");
        agent_ready.shutdown();
        println!("Shut down Pyroscope server");
    }
}

pub fn create_profiler_agent(pyroscope_server_addr: &str) -> Profiler {
    // Configure Pyroscope Agent
    let agent = PyroscopeAgent::builder(pyroscope_server_addr, "rusted_pipe")
        .backend(pprof_backend(PprofConfig::new().sample_rate(100)))
        .build()
        .expect("Cannot start Pyroscope server");
    let agent_running = agent.start().unwrap();
    println!("Sending Pyroscope data to {pyroscope_server_addr}");
    Profiler {
        profiler: agent_running,
    }
}

/// Creates tools for metrics and profiling.
///
/// Args
/// - prometheus_addr: Address where the prometheus server will be running locally. This is the server
/// that returns metrics in pull mode.
pub fn spawn_metrics_server(prometheus_addr: &str) -> MetricsServer {
    let binding = prometheus_addr.parse().unwrap();
    let exporter = prometheus_exporter::start(binding).unwrap();

    MetricsServer {
        _exporter: exporter,
    }
}

#[derive(Default, Clone)]
pub struct BufferMonitor {
    metrics: Option<GenericGauge<prometheus::core::AtomicI64>>
}



pub struct BufferMonitorBuilder{
    node_id: Option<String>
}

impl BufferMonitorBuilder {
    pub fn new(node_id: &str) -> Self {
        Self {
            node_id: Some(node_id.to_string())
        }
    }

    pub fn no_monitor() -> Self {
        Self {
            node_id: None
        }
    }


    pub fn make_channel(&self, channel_id: &str) -> BufferMonitor {
        if let Some(id) = self.node_id.as_ref() {
            BufferMonitor::new(id, channel_id)
        } else {
            BufferMonitor::default()
        }
        
    }
}

impl BufferMonitor {
    pub fn new(node_id: &str, channel_id: &str) -> Self {
        let metrics = SIZE_METRIC.with_label_values(&[node_id, channel_id]);
        Self {
            metrics: Some(metrics)
        }
    }

    pub fn observe(&mut self, size: i64) {
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.set(size);
        }
    }

    pub fn inc(&mut self) {
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.inc();
        }
    }

    pub fn dec(&mut self) {
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.dec();
        }
    }
}
