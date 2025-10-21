import os
import json
from datetime import datetime

import boto3
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = 'your-secret-key'  # Replace with something secure

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'tierbucket')
RGW_ENDPOINT = os.environ.get('RGW_ENDPOINT', 'https://s3.mad.eu.cephlabs.com')
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', 'user1')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', 'user1')

# For usage chart
BUCKET_QUOTA_MB = float(os.environ.get('BUCKET_QUOTA_MB', 20))

# ---------------------------------------------------------------------
# S3 Client
# ---------------------------------------------------------------------
s3 = boto3.client(
    's3',
    endpoint_url=RGW_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# ---------------------------------------------------------------------
# Helper Function: Gather Bucket Info
# ---------------------------------------------------------------------
def get_bucket_info():
    """
    Returns three things:
      1) A list of objects (with Key, Size, LastModified, StorageClass).
      2) Total bytes used in the bucket.
      3) A dict counting how many objects exist per StorageClass.
    """
    objects = []
    total_bytes = 0
    sc_counts = {}

    try:
        resp = s3.list_objects_v2(Bucket=BUCKET_NAME)
        for obj in resp.get('Contents', []):
            key = obj['Key']
            size = obj['Size']
            last_modified = obj['LastModified']
            total_bytes += size

            # HEAD each object to detect actual StorageClass
            sc = "STANDARD"
            try:
                head = s3.head_object(Bucket=BUCKET_NAME, Key=key)
                sc = head.get('StorageClass', 'STANDARD')
            except Exception:
                pass

            # Tally in sc_counts
            sc_counts[sc] = sc_counts.get(sc, 0) + 1

            objects.append({
                'Key': key,
                'Size': size,
                'LastModified': last_modified,
                'StorageClass': sc
            })

    except Exception as e:
        print(f"[WARN] Error listing bucket contents: {e}")

    return objects, total_bytes, sc_counts

# ---------------------------------------------------------------------
# Flask Routes
# ---------------------------------------------------------------------

@app.route('/', methods=['GET', 'POST'])
def index():
    """
    Main page:
      - Upload form
      - Charts for usage & SC distribution
      - Initial object listing
    """
    if request.method == 'POST':
        # Handle file upload
        upfile = request.files.get('file')
        if not upfile or upfile.filename == '':
            flash('No file selected.', 'warning')
            return redirect(request.url)

        try:
            filename = secure_filename(upfile.filename)
            s3_key = datetime.utcnow().strftime('%Y%m%d%H%M%S_') + filename
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=upfile.read(),
                ACL='public-read'
            )
            flash(f"File '{filename}' uploaded successfully!", 'success')
        except Exception as e:
            flash(f"Upload failed: {e}", 'danger')

        return redirect(url_for('index'))

    # For GET, show charts & object table
    objects, total_bytes, sc_counts = get_bucket_info()
    used_mb = round(total_bytes / (1024 * 1024), 2)
    usage_percentage = min((used_mb / BUCKET_QUOTA_MB) * 100, 100)

    return render_template(
        'index.html',
        rgw_endpoint=RGW_ENDPOINT,
        bucket_name=BUCKET_NAME,
        objects=objects,
        used_mb=used_mb,
        bucket_quota_mb=BUCKET_QUOTA_MB,
        usage_percentage=usage_percentage,
        sc_counts=sc_counts
    )

@app.route('/preview/<path:key>')
def preview(key):
    """
    Simple preview route: text is returned inline, images displayed,
    everything else gets a short message.
    """
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        content_type = obj.get('ContentType', '')
        data = obj['Body'].read()

        # Text-based
        if content_type.startswith('text') or key.lower().endswith(('.txt', '.log')):
            return data.decode('utf-8', errors='replace')
        # Image-based
        elif key.lower().endswith(('.png', '.jpg', '.jpeg', '.gif')):
            return f"""
            <html>
              <body>
                <h3>Preview: {key}</h3>
                <img src="{RGW_ENDPOINT}/{BUCKET_NAME}/{key}" alt="Image preview" />
              </body>
            </html>
            """
        else:
            return "Preview not available for this file type.", 400

    except Exception as e:
        return f"Error previewing file: {e}", 500

@app.route('/api/filelist', methods=['GET'])
def api_filelist():
    """
    Returns an array of objects with updated StorageClass so
    the table can be reloaded via JavaScript.
    """
    objects, total_bytes, sc_counts = get_bucket_info()
    # Convert LastModified to string
    for o in objects:
        o['LastModified'] = str(o['LastModified'])
    return jsonify(objects)

@app.route('/api/summary', methods=['GET'])
def api_summary():
    """
    Returns a JSON summary with sc_counts, usedMB, and usagePercentage
    so we can update the charts in real time.
    """
    objects, total_bytes, sc_counts = get_bucket_info()
    used_mb = round(total_bytes / (1024 * 1024), 2)
    usage_percentage = min((used_mb / BUCKET_QUOTA_MB) * 100, 100)

    return jsonify({
        "sc_counts": sc_counts,
        "used_mb": used_mb,
        "bucket_quota_mb": BUCKET_QUOTA_MB,
        "usage_percentage": usage_percentage
    })

@app.route('/healthz')
def healthz():
    """
    Basic health check endpoint.
    """
    return jsonify({"status": "OK"}), 200


# ---------------------------------------------------------------------
# Run the App
# ---------------------------------------------------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

