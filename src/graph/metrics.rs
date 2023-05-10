use bytes::Bytes;

use http_body_util::Full;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use prometheus::{Encoder, TextEncoder};

use pyroscope::pyroscope::PyroscopeAgentRunning;
use pyroscope::PyroscopeAgent;
use pyroscope_pprofrs::{pprof_backend, PprofConfig};

use std::net::TcpListener;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;

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

// An async function that consumes a request, does nothing with it and returns a
// response.
async fn metric_service(
    _: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::http::Error> {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    let mf = prometheus::gather();
    encoder.encode(&mf, &mut buffer).unwrap();
    Response::builder()
        .header(hyper::header::CONTENT_TYPE, encoder.format_type())
        .body(Full::new(Bytes::from(buffer)))
}

/// Creates a prometheus address on localhost:9001
pub fn default_prometheus_address() -> String {
    format!("{LOCALHOST}:9001")
}

/// Default pyroscope server address
pub fn default_pyroscope_address() -> String {
    format!("{LOCALHOST}:4040")
}

fn create_metrics_server(
    addr: &str,
) -> Result<TcpListener, Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).expect("Cannot create prometheus server");
    println!("Prometheus endpoint on http://{}", addr);
    return Ok(listener);
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
    join: thread::JoinHandle<()>,
    prometheus_stopper: Arc<AtomicBool>,
}

impl MetricsServer {
    pub fn stop(self) {
        self.prometheus_stopper
            .store(true, atomic::Ordering::Relaxed);
        self.join
            .join()
            .expect("Cannot stop Prometheus metrics thread");
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
///
/// - pyroscope_server_addr: Address of the remote Pyroscope server. The data is sent in push mode
/// to the server and usually adds 2% overhead.
pub fn spawn_metrics_server(prometheus_addr: &str) -> MetricsServer {
    let stopper_arc = Arc::new(AtomicBool::default());
    let stopper_clone = stopper_arc.clone();
    let prometheus_str = prometheus_addr.to_string();
    let handle = thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _ = runtime.block_on(runtime.spawn(async move {
            let listener = create_metrics_server(&prometheus_str)
                .expect("Cannot create prometheus metrics server");
            listener
                .set_nonblocking(true)
                .expect("Cannot set non blocking on prometheus server");
            let tokio_listener = tokio::net::TcpListener::from_std(listener)
                .expect("Cannot create tokio listener for prometheus metrics");
            while stopper_clone.load(atomic::Ordering::Relaxed) == false {
                if let Ok(streams) = tokio_listener.accept().await {
                    tokio::task::spawn(async move {
                        if let Err(err) = http1::Builder::new()
                            .serve_connection(streams.0, service_fn(metric_service))
                            .await
                        {
                            println!("Error serving connection: {:?}", err);
                        }
                    });
                } else {
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
            }
        }));
    });

    MetricsServer {
        join: handle,
        prometheus_stopper: stopper_arc.clone(),
    }
}
