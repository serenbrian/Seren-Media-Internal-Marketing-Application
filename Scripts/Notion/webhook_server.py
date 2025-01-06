from flask import Flask, request, jsonify
import subprocess
import os

app = Flask(__name__)

# Directory containing the Python scripts
script_directory = "/Users/brianhellemn/Scripts/Notion"

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    try:
        # Parse the incoming JSON payload
        data = request.json

        # Extract the "Script" rollup property
        script_name = data.get("properties", {}).get("Script", {}).get("rollup", {}).get("array", [{}])[0].get("name")

        if not script_name:
            return jsonify({"status": "error", "message": "Script property not found in webhook payload"}), 400

        # Construct the full path to the script
        script_path = os.path.join(script_directory, script_name)

        # Check if the script exists
        if not os.path.isfile(script_path):
            return jsonify({"status": "error", "message": f"Script '{script_name}' does not exist in {script_directory}"}), 400

        # Run the script as a subprocess
        subprocess.Popen(["python3", script_path])

        return jsonify({"status": "success", "message": f"Triggered script: {script_name}"}), 200

    except Exception as e:
        # Handle unexpected errors
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    # Bind to the host and port dynamically for Render
    port = int(os.environ.get("PORT", 5000))  # Use Render's PORT variable or default to 5000
    app.run(host='0.0.0.0', port=port)  # Bind to all interfaces
