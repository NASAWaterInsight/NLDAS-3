import logging
import re
import pandas as pd
from datetime import datetime
import threading
import time

class S3FSLogMonitor:
    def __init__(self):
        self.requests = []
        self.request_starts = {}  # Dictionary to store start times for each request
        self.lock = threading.Lock()
        self.setup_logging()
        
    def setup_logging(self):
        # Create a custom handler that will process logs
        class S3FSLogHandler(logging.Handler):
            def __init__(self, monitor):
                super().__init__()
                self.monitor = monitor
                
            def emit(self, record):
                if "Fetch:" in record.getMessage():
                    self.monitor.process_log(record.getMessage())
        
        # Get the s3fs logger
        logger = logging.getLogger('s3fs')
        logger.setLevel(logging.DEBUG)
        # Add our custom handler
        logger.addHandler(S3FSLogHandler(self))
    
    def process_log(self, log_message):
        # Extract data from the log message
        fetch_match = re.search(r"Fetch: (.+?), (\d+)-(\d+)", log_message)
        if fetch_match:
            key, start_byte, end_byte = fetch_match.groups()
            
            # Convert to integers
            start_byte = int(start_byte)
            end_byte = int(end_byte)
            
            # Calculate total bytes
            total_bytes = end_byte - start_byte + 1
            total_mb = total_bytes / (1024 * 1024)
            
            # Create a unique request identifier
            request_id = f"{key}_{start_byte}_{end_byte}"
            current_time = datetime.now()
            
            # Thread-safe addition to list
            with self.lock:
                if request_id not in self.request_starts:
                    # This is the start of the request
                    self.request_starts[request_id] = current_time
                else:
                    # This is the completion of the request
                    start_time = self.request_starts[request_id]
                    elapsed_ms = (current_time - start_time).total_seconds() * 1000
                    
                    self.requests.append({
                        'timestamp': current_time,
                        'key': key,
                        'start_byte': start_byte,
                        'end_byte': end_byte,
                        'bytes_range': f"{start_byte:,}-{end_byte:,}",
                        'total_bytes': total_bytes,
                        'total_mb': round(total_mb, 2),
                        'elapsed_ms': round(elapsed_ms, 1)
                    })
                    # Clean up the start time entry
                    del self.request_starts[request_id]

    def clear(self):
        """Clear all captured request data to start fresh monitoring."""
        with self.lock:
            self.requests = []
            self.request_starts = {}
        return "Monitor cleared. Ready for new requests."

    def get_summary_table(self):
        with self.lock:
            if not self.requests:
                return "No S3FS requests captured yet."
            
            df = pd.DataFrame(self.requests)
            
            # Select columns for display
            display_df = df[['timestamp', 'bytes_range', 'total_mb', 'elapsed_ms', 'key']]
            
            # Rename columns
            display_df = display_df.rename(columns={
                'timestamp': 'Timestamp',
                'bytes_range': 'Bytes Range',
                'total_mb': 'Size (MB)',
                'elapsed_ms': 'Elapsed (ms)',
                'key': 'Object Key'
            })
        self.clear()    
        return display_df
    