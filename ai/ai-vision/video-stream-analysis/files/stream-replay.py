import time
import base64
import json
import importlib.util
import oci
import streamlit as st
from streamlit_javascript import st_javascript
from PIL import Image
from io import BytesIO
import numpy as np
import cv2
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import logging, warnings
import pandas as pd
import altair as alt
from enum import Enum
from pathlib import Path

#------------------ Constants -----------------
OBJECT_LIMIT = 500
OCCUPANCY_WINDOW_SEC = 20  #time frame covered by the line graph for face detection 
FRAME_WIDTH = 700
FRAME_HEIGHT = 350

# ----------------- Enum -----------------
class DetectionMode(Enum):
    OBJECT = "Object Detection"
    FACE = "Face Detection"

# ----------------- Setup -----------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore")

st.set_page_config(page_title="OCI Vision Streaming", layout="wide", initial_sidebar_state="expanded")

with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# ----------------- Session Defaults -----------------
defaults = {
    "stream_job_ocid": "",
    "stream_job_ocid_input": "",
    "stream_source_ocid": "",
    "stream_group_ocid": "",
    "vision_private_endpoint_ocid": "",
    "compartment_id": "",
    "subnet_id": "",
    "camera_url": "",
    "bucket": "",
    "prefix": "",
    "os_namespace": "",
    "streaming": False,
    "occupancy_count": 0,
    "peak_occupancy": {"timestamp": "", "count": 0},
    "face_detection_timestamps": [],
    "occupancy_history": [],
    "object_counts": {},
    "mode": DetectionMode.FACE.value,  # default
    "start_stop_label": "▶️ Start Consumption"
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# OCI Instance Principal
oci_signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
oci_config = {"region": oci_signer.region}
service_endpoint = f"https://vision.aiservice.{oci_config['region']}.oci.oraclecloud.com"

stream_job_spec = importlib.util.spec_from_file_location(
    "stream_job_module",
    Path(__file__).with_name("stream-job.py")
)
if stream_job_spec is None or stream_job_spec.loader is None:
    raise ImportError("Could not load stream-job.py")
stream_job_module = importlib.util.module_from_spec(stream_job_spec)
stream_job_spec.loader.exec_module(stream_job_module)

# ----------------- Helpers -----------------
def get_base64_image(image_path):
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode()

#--------------------------------------------------------------------------------------#
def get_current_time():
    return datetime.now(timezone.utc)

#--------------------------------------------------------------------------------------#
def datetime_to_unix_ns(value):
    """Convert an aware datetime to a Unix timestamp in nanoseconds."""
    value = value.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    elapsed = value - epoch
    return (
        (elapsed.days * 86_400 + elapsed.seconds) * 1_000_000_000
        + elapsed.microseconds * 1_000
    )

#--------------------------------------------------------------------------------------#
def decode_image(image_data):
    image_bytes = base64.b64decode(image_data)
    image = Image.open(BytesIO(image_bytes))
    if image.mode not in ("RGB", "RGBA"):
        image = image.convert("RGB")
    return np.array(image)

#--------------------------------------------------------------------------------------#

def draw_objects_with_boxes(image, objects):
    h, w, _ = image.shape
    for obj in objects:
        pts = [(int(v['x'] * w), int(v['y'] * h)) for v in obj['boundingPolygon']['normalizedVertices']]
        x_coords = [pt[0] for pt in pts]
        y_coords = [pt[1] for pt in pts]
        x1, y1, x2, y2 = min(x_coords), min(y_coords), max(x_coords), max(y_coords)
        cv2.rectangle(image, (x1, y1), (x2, y2), (0, 255, 0), 2)
        label = f"{obj['name']} ({obj['confidence']:.2f})"
        cv2.putText(image, label, (x1, y1 - 10),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2, cv2.LINE_AA)
    return image

#--------------------------------------------------------------------------------------#

def blur_faces(image, faces):
    h, w, _ = image.shape
    for face in faces:
        pts = [(int(v['x'] * w), int(v['y'] * h)) for v in face['boundingPolygon']['normalizedVertices']]
        x_coords = [pt[0] for pt in pts]
        y_coords = [pt[1] for pt in pts]
        x1, y1, x2, y2 = min(x_coords), min(y_coords), max(x_coords), max(y_coords)
        cv2.rectangle(image, (x1, y1), (x2, y2), (0, 0, 255), 2)
    return image

#--------------------------------------------------------------------------------------#

def process_frame(message, mode):
    data = json.loads(message.replace("'", '"'))
    image = decode_image(data['imageData'])
    if mode == DetectionMode.OBJECT.value:
        objects = data.get("detectedObjects", [])
        if objects:
            image = draw_objects_with_boxes(image, objects)
        return image, objects
    elif mode == DetectionMode.FACE.value:
        faces = data.get("detectedFaces", [])
        if faces:
            image = blur_faces(image, faces)
        return image, faces

#--------------------------------------------------------------------------------------#

def update_analytics(faces):
    current_time = get_current_time()
    occupancy = len(faces)
    st.session_state.occupancy_history.append((current_time, occupancy))
    cutoff = current_time - timedelta(seconds=OCCUPANCY_WINDOW_SEC)
    st.session_state.occupancy_history = [
        (t, o) for t, o in st.session_state.occupancy_history if t >= cutoff
    ]
    st.session_state.occupancy_count = occupancy

#--------------------------------------------------------------------------------------#

def update_object_counts(objects):
    """Accumulate detected object counts with up to last 5 timestamps each."""
    current_time = get_current_time()
    # Initialize dict if not exists
    if "object_counts" not in st.session_state:
        st.session_state.object_counts = {}

    for obj in objects:
        name = obj["name"]
        if name not in st.session_state.object_counts:
            st.session_state.object_counts[name] = {"timestamps": []}
        st.session_state.object_counts[name]["timestamps"].append(current_time)

        # Keep only last 5 timestamps
        st.session_state.object_counts[name]["timestamps"] = st.session_state.object_counts[name]["timestamps"][-5:]


#--------------------------------------------------------------------------------------#

def consume_stream(namespace, bucket, prefix, client, frame_placeholder, chart_placeholder, mode,
                   replay_start_timestamp_ns, delay=0):
    try:
        object_prefix = prefix.rstrip("/") + "/" if prefix else ""
        start_object_name = f"{object_prefix}frame_{replay_start_timestamp_ns}.json"
        objs = client.list_objects(
            namespace,
            bucket,
            prefix=object_prefix,
            start=start_object_name,
            limit=OBJECT_LIMIT
        ).data.objects
        if not objs:
            frame_placeholder.markdown('<div class="video-box"><p>⚠️ No frames found.</p></div>', unsafe_allow_html=True)
            return

        chart_ph = chart_placeholder.empty()
        objs = sorted(objs, key=lambda o: o.name)

        for obj in objs:
            content = client.get_object(namespace, bucket, obj.name).data.content.decode("utf-8")
            frame, detections = process_frame(content, mode)

            if mode == DetectionMode.OBJECT.value:
                update_object_counts(detections)
            elif mode == DetectionMode.FACE.value:
                update_analytics(detections)

            rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            _, buffer = cv2.imencode(".jpg", rgb_frame)
            frame_base64 = base64.b64encode(buffer).decode()

            frame_placeholder.markdown(
                f'<div class="video-box"><img src="data:image/jpeg;base64,{frame_base64}" '
                'style="width:100%; height:100%; object-fit:contain;" /></div>',
                unsafe_allow_html=True
            )

            # Different detections require different Analytics 
            if mode == DetectionMode.OBJECT.value and st.session_state.object_counts:
                data = []
                for obj_name, info in st.session_state.object_counts.items():
                    timestamps_str = ", ".join([t.strftime("%H:%M:%S") for t in info["timestamps"]])
                    data.append([obj_name, timestamps_str])
                df_counts = pd.DataFrame(data, columns=["Object Detected", "Timestamps"])
                chart_ph.dataframe(df_counts, use_container_width=True)

            elif mode == DetectionMode.FACE.value and st.session_state.occupancy_history:
                df_occupancy = pd.DataFrame(st.session_state.occupancy_history, columns=["Time", "Count"])
                df_occupancy["Time"] = pd.to_datetime(df_occupancy["Time"])
                peak_row = df_occupancy.loc[df_occupancy["Count"].idxmax()]
                peak_time = peak_row["Time"]
                peak_count = peak_row["Count"]

                col1, col2 = chart_ph.columns([2, 1])
                with col1:
                    chart = alt.Chart(df_occupancy).mark_line().encode(
                        x="Time:T",
                        y=alt.Y("Count:Q", scale=alt.Scale(domain=[0, 10]))
                    ).properties(width=FRAME_WIDTH, height=FRAME_HEIGHT, title="Occupancy Over Time")
                    col1.altair_chart(chart, use_container_width=False)
                with col2:
                    col2.markdown("### Peak Occupancy")
                    col2.markdown(f"**Count:** {peak_count}")
                    col2.markdown(f"**Time:** {peak_time.strftime('%Y-%m-%d %H:%M:%S')}")

            time.sleep(delay)

    except Exception as e:
        st.error(f"Error reading stored frames: {e}")



#--------------------------------------------------------------------------------------#
#-----------------------------------PAGE SETUP-----------------------------------------#
#--------------------------------------------------------------------------------------#



logo_base64 = get_base64_image('../media/oracle_logo.png')
st.markdown(
    f"""
    <div class="banner">
        <img src="data:image/png;base64,{logo_base64}" alt="logo">
        <h1>OCI Vision Streaming</h1>
    </div>
    """,
    unsafe_allow_html=True
)
st.markdown("<br><br><br>", unsafe_allow_html=True)

col1, col2 = st.columns([1, 3])

with col1:
    browser_timezone = st_javascript("Intl.DateTimeFormat().resolvedOptions().timeZone")
    if not isinstance(browser_timezone, str) or not browser_timezone:
        browser_timezone = "UTC"

    try:
        browser_tz = ZoneInfo(browser_timezone)
    except ZoneInfoNotFoundError:
        browser_timezone = "UTC"
        browser_tz = timezone.utc

    st.caption(f"Browser timezone: {browser_timezone}")

    st.session_state.mode = st.radio(
        "Detection Mode",
        [DetectionMode.OBJECT.value, DetectionMode.FACE.value],
        index=0
    )

    st.text_input("Existing Stream Job OCID (optional)", key="stream_job_ocid_input")
    st.text_input("Compartment OCID", key="compartment_id")
    st.text_input("Subnet OCID", key="subnet_id")
    st.text_input("Camera URL", key="camera_url")
    st.session_state.bucket = st.text_input("Bucket Name")
    st.session_state.prefix = st.text_input("Prefix")
    st.session_state.os_namespace = st.text_input("Object Storage Namespace")

    start_job_column, stop_job_column = st.columns(2)
    if start_job_column.button("Start stream job", use_container_width=True):
        existing_job_id = st.session_state.stream_job_ocid_input.strip()
        required_job_inputs = [
            st.session_state.compartment_id,
            st.session_state.subnet_id,
            st.session_state.camera_url,
            st.session_state.os_namespace,
            st.session_state.bucket,
        ]
        if not existing_job_id and not all(required_job_inputs):
            st.error("Enter an existing Stream Job OCID or provide the compartment, subnet, camera URL, namespace, and bucket to create one.")
        else:
            try:
                with st.spinner("Creating or starting the OCI Vision stream job..."):
                    stream_video = stream_job_module.StreamVideo(
                        compartment_id=st.session_state.compartment_id,
                        subnet_id=st.session_state.subnet_id,
                        camera_url=st.session_state.camera_url,
                        namespace=st.session_state.os_namespace,
                        bucket=st.session_state.bucket,
                        prefix=st.session_state.prefix,
                        oci_config=oci_config,
                        service_endpoint=service_endpoint,
                        signer=oci_signer,
                    )

                    if existing_job_id:
                        stream_job_id = existing_job_id
                    else:
                        if not st.session_state.vision_private_endpoint_ocid:
                            active_endpoints = stream_video.client.list_vision_private_endpoints(
                                compartment_id=st.session_state.compartment_id,
                                lifecycle_state="ACTIVE",
                            ).data.items
                            matching_endpoint = next(
                                (endpoint for endpoint in active_endpoints
                                 if endpoint.subnet_id == st.session_state.subnet_id),
                                None,
                            )
                            st.session_state.vision_private_endpoint_ocid = (
                                matching_endpoint.id if matching_endpoint
                                else stream_video.create_private_endpoint()
                            )

                        if not st.session_state.stream_source_ocid:
                            st.session_state.stream_source_ocid = stream_video.create_Stream_Source(
                                st.session_state.vision_private_endpoint_ocid
                            )
                        if not st.session_state.stream_job_ocid:
                            st.session_state.stream_job_ocid = stream_video.create_Stream_Job(
                                st.session_state.stream_source_ocid
                            )
                        if not st.session_state.stream_group_ocid:
                            st.session_state.stream_group_ocid = stream_video.create_Stream_Group(
                                st.session_state.stream_source_ocid
                            )
                        stream_job_id = st.session_state.stream_job_ocid

                    stream_video.start_Stream_Job(stream_job_id)
                    st.session_state.stream_job_ocid = stream_job_id
                st.success(f"Stream job started: {st.session_state.stream_job_ocid}")
            except (Exception, SystemExit) as error:
                st.error(f"Could not start stream job: {error}")

    if stop_job_column.button("Stop stream job", use_container_width=True):
        stream_job_id = (
            st.session_state.stream_job_ocid_input.strip()
            or st.session_state.stream_job_ocid
        )
        if not stream_job_id:
            st.error("Enter a Stream Job OCID or start a stream job first.")
        else:
            try:
                with st.spinner("Stopping the OCI Vision stream job..."):
                    stream_video = stream_job_module.StreamVideo(
                        compartment_id=st.session_state.compartment_id,
                        subnet_id=st.session_state.subnet_id,
                        camera_url=st.session_state.camera_url,
                        namespace=st.session_state.os_namespace,
                        bucket=st.session_state.bucket,
                        prefix=st.session_state.prefix,
                        oci_config=oci_config,
                        service_endpoint=service_endpoint,
                        signer=oci_signer,
                    )
                    stream_video.stop_Stream_Job(stream_job_id)
                st.success(f"Stream job stopped: {stream_job_id}")
            except (Exception, SystemExit) as error:
                st.error(f"Could not stop stream job: {error}")

    now_local = datetime.now(browser_tz)
    if "replay_date" not in st.session_state:
        st.session_state.replay_date = now_local.date()
    if "replay_time" not in st.session_state:
        st.session_state.replay_time = now_local.time().replace(microsecond=0)

    if st.button("Set to now", help="Set the replay date and time to the current browser-local time"):
        now_local = datetime.now(browser_tz)
        st.session_state.replay_date = now_local.date()
        st.session_state.replay_time = now_local.time().replace(microsecond=0)

    replay_date = st.date_input("Replay Start Date", key="replay_date")
    replay_time = st.time_input(
        "Replay Start Time (Browser timezone)",
        key="replay_time",
        step=timedelta(minutes=1)
    )
    replay_start_local = datetime.combine(replay_date, replay_time, tzinfo=browser_tz)
    replay_start = replay_start_local.astimezone(timezone.utc)
    st.session_state.replay_start_timestamp_ns = datetime_to_unix_ns(replay_start)
    st.caption(f"Replay start (UTC): {replay_start:%Y-%m-%d %H:%M:%S %Z}")
    st.caption(f"Replay start (Unix ns): {st.session_state.replay_start_timestamp_ns}")

with col2:
    frame_placeholder = st.empty()
    frame_placeholder.markdown('<div class="video-box"><p>🎥 Waiting for stream...</p></div>', unsafe_allow_html=True)

def toggle_streaming():
    st.session_state.streaming = not st.session_state.streaming
    st.session_state.start_stop_label = "⏹️ Stop Consumption" if st.session_state.streaming else "▶️ Start Consumption"

st.button(st.session_state.start_stop_label, on_click=toggle_streaming)

st.markdown("---")
chart_placeholder = st.empty()

try:
    if st.session_state.streaming:
        if not all([st.session_state.bucket, st.session_state.prefix, st.session_state.os_namespace]):
            st.error("Please provide all required Stream Job and Object Storage details")
        else:
            storage_client = oci.object_storage.ObjectStorageClient(
                oci_config,
                signer=oci_signer
            )
            consume_stream(
                st.session_state.os_namespace,
                st.session_state.bucket,
                st.session_state.prefix,
                storage_client,
                frame_placeholder,
                chart_placeholder,
                st.session_state.mode,
                st.session_state.replay_start_timestamp_ns
            )
    else:
        frame_placeholder.markdown('<div class="video-box"><p>⏹️ Stream stopped</p></div>', unsafe_allow_html=True)

except Exception as e:
    st.error(f"Operation failed: {e}")

