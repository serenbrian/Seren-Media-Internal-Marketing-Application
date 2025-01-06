from flask import Flask, request, jsonify
import subprocess
import os
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Directory containing the Python scripts (relative to this file)
script_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')

ALLOWED_SCRIPTS = {
    "your_script.py",
    "another_script.py",
    "metricool_fetch_analytics_data.py",  # Added script
    # Add other allowed script names here
}

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    try:
        # Parse the incoming JSON payload
        data = request.json
        logger.info(f"Received data: {data}")

        # Extract the "Script" rollup property
        script_name = data.get("properties", {}).get("Script", {}).get("rollup", {}).get("array", [{}])[0].get("name")
        logger.info(f"Extracted script name: '{script_name}'")

        # Log the current ALLOWED_SCRIPTS
        logger.info(f"Allowed scripts: {ALLOWED_SCRIPTS}")

        if not script_name:
            logger.error("Script property not found in webhook payload")
            return jsonify({"status": "error", "message": "Script property not found in webhook payload"}), 400

        if script_name not in ALLOWED_SCRIPTS:
            logger.error(f"Unauthorized script attempted: '{script_name}'")
            return jsonify({"status": "error", "message": "Unauthorized script"}), 403

        # Construct the full path to the script
        script_path = os.path.join(script_directory, script_name)
        logger.info(f"Constructed script path: '{script_path}'")

        # Check if the script exists
        if not os.path.isfile(script_path):
            logger.error(f"Script does not exist: '{script_path}'")
            return jsonify({"status": "error", "message": f"Script '{script_name}' does not exist in {script_directory}"}), 400

        # Run the script as a subprocess
        subprocess.Popen(["python3", script_path])
        logger.info(f"Triggered script: '{script_name}'")

        return jsonify({"status": "success", "message": f"Triggered script: {script_name}"}), 200

    except Exception as e:
        logger.exception("An unexpected error occurred")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    # Bind to the host and port dynamically for Render or local testing
    port = int(os.environ.get("PORT", 5000))  # Use Render's PORT variable or default to 5000
    app.run(host='0.0.0.0', port=port)  # Bind to all interfaces
