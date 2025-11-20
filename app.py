#!/usr/bin/env python3
"""
Kaggle Notebook Executor - Fixed Single File
Runs every 3 minutes via cron-job.org
"""

import os
import subprocess
import json
import sys
from datetime import datetime
from pathlib import Path
import shutil

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

SOURCE_ACCOUNT = {
    "username": "shreevathsbbhh",
    "key": "9f167cdee8a045c97ca6a2f82c6701f9"
}

DEST_ACCOUNT = {
    "username": "distinct4exist",
    "key": "c2767798395ca8c007e931d6f9d42752"
}

NOTEBOOKS = [
    {"source_slug": "shreevathsbbhh/new-19-1", "notebook_name": "new-19-1", "dest_slug": "distinct4exist/new-19-1"},
    {"source_slug": "shreevathsbbhh/new-19-2", "notebook_name": "new-19-2", "dest_slug": "distinct4exist/new-19-2"}
]

# ═══════════════════════════════════════════════════════════════════════════
# FLASK SETUP (import early, before kaggle)
# ═══════════════════════════════════════════════════════════════════════════

try:
    from flask import Flask, jsonify
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "flask"])
    from flask import Flask, jsonify

app = Flask(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# CORE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def log(msg, symbol="ℹ️"):
    timestamp = datetime.utcnow().strftime('%H:%M:%S')
    print(f"[{timestamp}] {symbol} {msg}")
    sys.stdout.flush()

def setup_kaggle_auth(account):
    """Setup Kaggle authentication - must be called before importing kaggle"""
    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(exist_ok=True)
    kaggle_json = kaggle_dir / "kaggle.json"
    
    with open(kaggle_json, 'w') as f:
        json.dump({"username": account["username"], "key": account["key"]}, f)
    
    kaggle_json.chmod(0o600)
    log(f"Configured: {account['username']}", "🔑")

def run_cmd(cmd, timeout=120):
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, 
            text=True, timeout=timeout
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Timeout"
    except Exception as e:
        return False, "", str(e)

def execute_notebook(nb):
    log("═" * 70, "")
    log(f"📓 NOTEBOOK: {nb['notebook_name']}", "")
    log("═" * 70, "")
    
    temp_dir = Path(f"./temp_{nb['notebook_name']}")
    original_dir = os.getcwd()
    
    try:
        # Step 1: Pull from source
        setup_kaggle_auth(SOURCE_ACCOUNT)
        
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir()
        
        log("Pulling from source...", "📥")
        success, stdout, stderr = run_cmd(
            f"kaggle kernels pull {nb['source_slug']} -p {temp_dir} -m"
        )
        
        if not success:
            log(f"Pull failed: {stderr[:200]}", "❌")
            return False
        
        log("Pull successful", "✅")
        
        # Step 2: Update metadata
        metadata_file = temp_dir / "kernel-metadata.json"
        
        if not metadata_file.exists():
            log("Metadata file not found", "❌")
            return False
        
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        metadata.update({
            'id': nb['dest_slug'],
            'slug': nb['notebook_name'],
            'title': f"{nb['notebook_name']}-{timestamp}"
        })
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        log(f"Metadata updated: {nb['notebook_name']}-{timestamp}", "📝")
        
        # Step 3: Push to destination
        setup_kaggle_auth(DEST_ACCOUNT)
        
        os.chdir(temp_dir)
        
        log("Pushing to destination...", "📤")
        success, stdout, stderr = run_cmd("kaggle kernels push")
        
        os.chdir(original_dir)
        
        if not success:
            log(f"Push failed: {stderr[:200]}", "❌")
            return False
        
        log("Push successful", "✅")
        log(f"URL: https://www.kaggle.com/code/{nb['dest_slug']}", "🔗")
        
        return True
        
    except Exception as e:
        log(f"Error: {str(e)[:200]}", "❌")
        return False
        
    finally:
        if os.getcwd() != original_dir:
            os.chdir(original_dir)
        if temp_dir.exists():
            try:
                shutil.rmtree(temp_dir)
            except:
                pass

def execute_all():
    start = datetime.utcnow()
    log(f"🚀 EXECUTION STARTED: {start.isoformat()}", "")
    
    results = []
    for i, nb in enumerate(NOTEBOOKS):
        success = execute_notebook(nb)
        results.append(success)
        
        if i < len(NOTEBOOKS) - 1:
            log("Waiting 5 seconds...", "⏳")
            import time
            time.sleep(5)
    
    success_count = sum(results)
    duration = (datetime.utcnow() - start).total_seconds()
    
    log("═" * 70, "")
    log(f"COMPLETED: {success_count}/{len(results)} successful | {duration:.1f}s", "✅" if success_count == len(results) else "⚠️")
    log("═" * 70, "")
    
    return {
        'status': 'completed',
        'successful': success_count,
        'failed': len(results) - success_count,
        'total': len(results),
        'duration_seconds': round(duration, 2),
        'timestamp': datetime.utcnow().isoformat(),
        'results': [
            {'notebook': NOTEBOOKS[i]['notebook_name'], 'success': results[i]}
            for i in range(len(results))
        ]
    }

# ═══════════════════════════════════════════════════════════════════════════
# FLASK ROUTES
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/')
def home():
    return jsonify({
        'status': 'online',
        'service': 'Kaggle Notebook Executor',
        'notebooks': len(NOTEBOOKS),
        'interval': '3 minutes (testing)',
        'endpoints': {
            'trigger': '/trigger',
            'health': '/health'
        }
    })

@app.route('/trigger', methods=['GET', 'POST'])
def trigger():
    return jsonify(execute_all())

@app.route('/health')
def health():
    return jsonify({'status': 'healthy', 'time': datetime.utcnow().isoformat()})

# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    log(f"🌐 Starting server on port {port}", "")
    
    # Create kaggle config on startup to prevent import errors
    setup_kaggle_auth(SOURCE_ACCOUNT)
    
    app.run(host='0.0.0.0', port=port, debug=False)
