use super::Processor;
use crate::channels::ChannelID;
use crate::channels::WriteChannel;
use crate::packet::PacketSet;
use crate::DataVersion;
use crate::RustedPipeError;
use leptess::capi::{TessPageIteratorLevel, TessPageIteratorLevel_RIL_WORD};
use leptess::tesseract;
use leptess::tesseract::TessApi;
use opencv::core::min_max_loc;
use opencv::core::Point;
use opencv::core::Point2f;
use opencv::core::Range;
use opencv::core::Rect;
use opencv::core::Scalar;
use opencv::core::Size;
use opencv::core::ToInputOutputArray;
use opencv::core::Vector;
use opencv::core::BORDER_CONSTANT;
use opencv::core::CV_32F;
use opencv::dnn::blob_from_image;
use opencv::dnn::nms_boxes;
use opencv::dnn::read_net;
use opencv::dnn::read_net_from_darknet;
use opencv::dnn::Net;
use opencv::dnn::TextDetectionModel_EAST;
use opencv::dnn::TextDetectionModel_EASTTrait;
use opencv::dnn::TextRecognitionModel;
use opencv::dnn::DNN_BACKEND_OPENCV;
use opencv::dnn::DNN_TARGET_CPU;
use opencv::imgcodecs::imread;
use opencv::imgcodecs::imwrite;
use opencv::imgcodecs::IMREAD_COLOR;
use opencv::imgproc::cvt_color;
use opencv::imgproc::get_perspective_transform;
use opencv::imgproc::put_text;
use opencv::imgproc::warp_perspective;
use opencv::imgproc::COLOR_BGR2GRAY;
use opencv::imgproc::FONT_HERSHEY_PLAIN;
use opencv::imgproc::INTER_LINEAR;
use opencv::imgproc::LINE_8;
use opencv::imgproc::{rectangle, LineTypes};
use opencv::prelude::Mat;
use opencv::prelude::MatTraitConst;
use opencv::prelude::MatTraitManual;
use opencv::prelude::ModelTrait;
use opencv::prelude::NetTrait;
use opencv::prelude::NetTraitConst;
use opencv::prelude::TextDetectionModelTraitConst;
use opencv::prelude::TextRecognitionModelTrait;
use opencv::prelude::TextRecognitionModelTraitConst;
use std::ffi::CString;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

pub struct BoundinBoxRender {
    id: String,
}
impl BoundinBoxRender {
    pub fn default() -> Self {
        Self {
            id: "BoundinBoxRender".to_string(),
        }
    }
}

impl Processor for BoundinBoxRender {
    fn handle(
        &mut self,
        mut _input: PacketSet,
        output_channel: Arc<Mutex<WriteChannel>>,
    ) -> Result<(), RustedPipeError> {
        let mut bboxes_packet = _input
            .get_channel_owned::<Vector<Rect>>(&ChannelID::from("cars"))
            .unwrap();
        let plates_packet = _input
            .get_channel_owned::<Vec<CarWithText>>(&ChannelID::from("plates"))
            .unwrap();
        let bboxes = bboxes_packet.data.as_mut();
        //bboxes.extend(plates_packet.data.as_ref());
        let mut image = _input
            .get_channel_owned::<Mat>(&ChannelID::from("image"))
            .unwrap();
        let color = Scalar::from((255.0, 0.0, 0.0));
        let thikness_px = 2;

        let mut im_array = image.data.input_output_array().unwrap();

        for bbox in bboxes.iter() {
            for plate in plates_packet.data.iter() {
                let intersection = bbox & plate.car;
                let plate_text = plate.plate.as_ref().unwrap();
                if intersection.size() >= plate.car.size() {
                    let header = Rect::new(bbox.x, bbox.y, bbox.width, 20);
                    rectangle(
                        &mut im_array,
                        header,
                        color,
                        -1,
                        LineTypes::LINE_4 as i32,
                        0,
                    )
                    .unwrap();
                    put_text(
                        &mut im_array,
                        plate_text,
                        Point::new(bbox.x, bbox.y + 20),
                        FONT_HERSHEY_PLAIN,
                        2.0,
                        Scalar::from((255.0, 255.0, 255.0)),
                        2,
                        LINE_8,
                        false,
                    )
                    .unwrap();
                }
            }

            rectangle(
                &mut im_array,
                bbox,
                color,
                thikness_px,
                LineTypes::LINE_4 as i32,
                0,
            )
            .unwrap();
        }

        let params = Vector::<i32>::default();
        imwrite(
            format!(
                "output_{}.jpg",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            )
            .as_str(),
            &im_array,
            &params,
        )
        .unwrap();
        Ok(())
    }

    fn id(&self) -> &String {
        return &self.id;
    }
}

pub struct CarDetector {
    classifier: Net,
    id: String,
}

impl CarDetector {
    pub fn default() -> Self {
        let mut classifier = read_net_from_darknet(
            "vehicle-detection-classification-opencv/yolov3.cfg",
            "vehicle-detection-classification-opencv/yolov3.weights",
        )
        .unwrap();

        classifier
            .set_preferable_backend(DNN_BACKEND_OPENCV)
            .unwrap();
        classifier.set_preferable_target(DNN_TARGET_CPU).unwrap();
        return CarDetector {
            classifier,
            id: "CarDetector".to_string(),
        };
    }

    fn post_process(&self, img_rows: i32, img_cols: i32, outputs: &Vector<Mat>) -> Vector<Rect> {
        let mut confidences = Vector::<f32>::default();
        let mut boxes = Vector::<Rect>::default();

        for data in outputs {
            for j in 0..data.rows() {
                let mut scores = data
                    .row(j)
                    .unwrap()
                    .col_range(&Range::new(5, data.cols()).unwrap())
                    .unwrap();

                let mut min_val_p = 0.0;
                let mut max_loc_p = Point::default();
                let min_val = None;
                let max_val = Some(&mut min_val_p);
                let min_loc = None;
                let max_loc = Some(&mut max_loc_p);
                let scores_ia = &scores.input_output_array().unwrap();
                min_max_loc(
                    scores_ia,
                    min_val,
                    max_val,
                    min_loc,
                    max_loc,
                    &Mat::default().input_output_array().unwrap(),
                )
                .unwrap();
                let max_conf = min_val_p as f32;
                if max_conf > 0.5 && max_loc_p.x == 2 {
                    let center_x: i32 =
                        (data.at_2d::<f32>(j, 0).unwrap() * img_cols as f32).round() as i32;
                    let center_y: i32 =
                        (data.at_2d::<f32>(j, 1).unwrap() * img_rows as f32).round() as i32;
                    let width: i32 =
                        (data.at_2d::<f32>(j, 2).unwrap() * img_cols as f32).round() as i32;
                    let height: i32 =
                        (data.at_2d::<f32>(j, 3).unwrap() * img_rows as f32).round() as i32;
                    let left = center_x - width / 2;
                    let top = center_y - height / 2;

                    confidences.push(max_conf);
                    boxes.push(Rect::new(left, top, width, height));
                }
            }
        }

        let mut indices = Vector::<i32>::default();
        nms_boxes(&boxes, &confidences, 0.5, 0.4, &mut indices, 1.0, 0).unwrap();

        let mut output = Vector::<Rect>::default();
        for i in indices {
            output.push(boxes.get(i as usize).unwrap());
        }

        return output;
    }
}

unsafe impl Send for CarDetector {}
unsafe impl Sync for CarDetector {}

impl Processor for CarDetector {
    fn handle(
        &mut self,
        mut _input: PacketSet,
        output_channel: Arc<Mutex<WriteChannel>>,
    ) -> Result<(), RustedPipeError> {
        let mut image_packet = _input.get_owned::<Mat>(0).unwrap();
        let image = image_packet.data.as_mut();

        let input_size = 416;

        let mut blob = blob_from_image(
            image,
            1.0 / 255.0,
            Size::new(input_size, input_size),
            Scalar::default(),
            true,
            false,
            CV_32F,
        )
        .unwrap();

        // Set the input of the network
        self.classifier
            .set_input(&mut blob, "", 1.0, Scalar::default())
            .unwrap();

        let output_names = self.classifier.get_unconnected_out_layers_names().unwrap();

        let mut output = Vector::<Mat>::default();
        self.classifier.forward(&mut output, &output_names).unwrap();
        let out = self.post_process(image.rows(), image.cols(), &output);

        output_channel
            .lock()
            .unwrap()
            .write(&ChannelID::from("cars"), out, &image_packet.version)
            .unwrap();

        Ok(())
    }

    fn id(&self) -> &String {
        return &self.id;
    }
}

pub struct ImageReader {
    id: String,
}
impl ImageReader {
    pub fn default() -> Self {
        Self {
            id: "ImageReader".to_string(),
        }
    }
}

impl Processor for ImageReader {
    fn handle(
        &mut self,
        mut _input: PacketSet,
        output_channel: Arc<Mutex<WriteChannel>>,
    ) -> Result<(), RustedPipeError> {
        let image = imread("./carsplate.jpeg", IMREAD_COLOR).unwrap();
        output_channel
            .lock()
            .unwrap()
            .write(&ChannelID::from("image"), image, &DataVersion::from_now())
            .unwrap();
        //Err(RustedPipeError::EndOfStream())
        Ok(())
    }

    fn id(&self) -> &String {
        return &self.id;
    }
}

pub struct DnnOcrReader {
    id: String,
    network: TextDetectionModel_EAST,
    ocr: TextRecognitionModel,
}
impl DnnOcrReader {
    pub fn default() -> Self {
        let net = read_net("./frozen_east_text_detection.pb", "", "").unwrap();
        let mut net = TextDetectionModel_EAST::new(&net).unwrap();

        // Set the input of the network
        net.set_confidence_threshold(0.5)
            .unwrap()
            .set_nms_threshold(0.4)
            .unwrap();

        let scale = 1.0;
        let mean = Scalar::from((123.68, 116.78, 103.94));
        let input_size = Size::new(320, 320);

        net.set_input_params(scale, input_size, mean, true, false)
            .unwrap();

        let scale = 1.0 / 127.5;
        let mean = Scalar::from((127.5, 127.5, 127.5));
        let input_size = Size::new(100, 32);

        let mut ocr = TextRecognitionModel::from_file("CRNN_VGG_BiLSTM_CTC.onnx", "").unwrap();
        ocr.set_decode_type("CTC-greedy").unwrap();

        let mut vocabulary = Vector::<String>::default();
        let file = File::open("alphabet_36.txt").unwrap();
        let reader = BufReader::new(file);

        for line in reader.lines() {
            vocabulary.push(&line.unwrap());
        }
        ocr.set_vocabulary(&vocabulary).unwrap();
        ocr.set_input_params(scale, input_size, mean, false, false)
            .unwrap();

        Self {
            id: "DnnOcrReader".to_string(),
            network: net,
            ocr,
        }
    }

    fn reshape_plate(&self, image: &Mat, rect: &Vector<Point>) -> Mat {
        let output_size = Size::new(100, 32);
        let mut image_2f = Mat::default();
        image.convert_to(&mut image_2f, CV_32F, 1.0, 0.0).unwrap();
        let mut rect_2f = Vector::<Point2f>::default();
        for p in rect {
            rect_2f.push(Point2f::new(p.x as f32, p.y as f32));
        }

        let mut target_rect_2f = Vector::<Point2f>::default();
        target_rect_2f.push(Point2f::new(0.0, output_size.height as f32 - 1.0));
        target_rect_2f.push(Point2f::new(0.0, 0.0));
        target_rect_2f.push(Point2f::new(output_size.width as f32 - 1.0, 0.0));
        target_rect_2f.push(Point2f::new(
            output_size.width as f32 - 1.0,
            output_size.height as f32 - 1.0,
        ));

        let perspective = get_perspective_transform(&rect_2f, &target_rect_2f, 0).unwrap();
        let mut output = Mat::default();
        println!("{:?}", perspective);
        warp_perspective(
            &image_2f,
            &mut output,
            &perspective,
            output_size,
            INTER_LINEAR,
            BORDER_CONSTANT,
            Scalar::default(),
        )
        .unwrap();
        return output;
    }
}

unsafe impl Send for DnnOcrReader {}
unsafe impl Sync for DnnOcrReader {}

impl Processor for DnnOcrReader {
    fn handle(
        &mut self,
        mut _input: PacketSet,
        output_channel: Arc<Mutex<WriteChannel>>,
    ) -> Result<(), RustedPipeError> {
        let mut image_packet = _input.get_owned::<Mat>(0).unwrap();
        let image = image_packet.data.as_mut();
        let mut grey = Mat::default();
        cvt_color(image, &mut grey, COLOR_BGR2GRAY, 0).unwrap();

        let mut output = Vector::<Vector<Point>>::default();
        self.network.detect(image, &mut output).unwrap();

        let mut out_rect = vec![];

        for r in output {
            let rect = Rect::new(
                r.get(1).unwrap().x,
                r.get(1).unwrap().y,
                r.get(3).unwrap().x - r.get(1).unwrap().x,
                r.get(3).unwrap().y - r.get(1).unwrap().y,
            );

            let cropped = self.reshape_plate(&grey, &r);
            let result = self.ocr.recognize(&cropped).unwrap();
            out_rect.push(CarWithText::new(Some(result), rect));
        }

        output_channel
            .lock()
            .unwrap()
            .write(&ChannelID::from("plates"), out_rect, &image_packet.version)
            .unwrap();

        Ok(())
    }

    fn id(&self) -> &String {
        return &self.id;
    }
}

pub struct OcrReader {
    id: String,
    api: TessApi,
}
impl OcrReader {
    pub fn default() -> Self {
        let mut api = tesseract::TessApi::new(Some("./"), "eng").unwrap();
        let data_path_cstr = CString::new("./").unwrap();
        let lang = CString::new("eng").unwrap();
        api.raw
            .init_4(Some(data_path_cstr.as_ref()), Some(lang.as_ref()), 0)
            .unwrap();

        Self {
            id: "OcrReader".to_string(),
            api: api,
        }
    }
}

unsafe impl Send for OcrReader {}
unsafe impl Sync for OcrReader {}

impl Processor for OcrReader {
    fn handle(
        &mut self,
        mut _input: PacketSet,
        output_channel: Arc<Mutex<WriteChannel>>,
    ) -> Result<(), RustedPipeError> {
        let mut image_packet = _input
            .get_channel_owned::<Mat>(&ChannelID::from("image"))
            .unwrap();
        let image = image_packet.data.as_mut();
        let mut grey = Mat::default();
        cvt_color(image, &mut grey, COLOR_BGR2GRAY, 0).unwrap();

        let cols = image.cols();
        let rows = image.rows();
        self.api
            .raw
            .set_image(&grey.data_bytes_mut().unwrap(), cols, rows, 1, cols)
            .unwrap();

        let boxes = self
            .api
            .get_component_images(TessPageIteratorLevel_RIL_WORD, true)
            .unwrap();

        let mut output = Vector::<Rect>::default();
        for i in 0..boxes.get_n() {
            let b = boxes.get_box(i).unwrap();
            let r = *b.as_ref();
            output.push(Rect::new(r.x, r.y, r.w, r.h));
        }

        output_channel
            .lock()
            .unwrap()
            .write(&ChannelID::from("plates"), output, &image_packet.version)
            .unwrap();
        Ok(())
    }

    fn id(&self) -> &String {
        return &self.id;
    }
}

pub struct PlateAssigner {
    id: String,
}

#[derive(Clone)]
struct CarWithText {
    plate: Option<String>,
    car: Rect,
}

impl CarWithText {
    fn new(plate: Option<String>, car: Rect) -> Self {
        return Self { plate, car };
    }
}
