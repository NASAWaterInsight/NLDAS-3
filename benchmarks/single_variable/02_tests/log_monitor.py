import logging
import re
import pandas as pd
from datetime import datetime
import threading
import time

class S3FSLogMonitor:
    def __init__(self):
        self.requests = []
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
            
            # Thread-safe addition to list
            with self.lock:
                self.requests.append({
                    'timestamp': datetime.now(),
                    'key': key,
                    'start_byte': start_byte,
                    'end_byte': end_byte,
                    'bytes_range': f"{start_byte:,}-{end_byte:,}",
                    'total_bytes': total_bytes,
                    'total_mb': round(total_mb, 2)                
                })
            

    def clear(self):
        """Clear all captured request data to start fresh monitoring."""
        with self.lock:
            self.requests = []
        return "Monitor cleared. Ready for new requests."

    def get_summary_table(self):
        with self.lock:
            if not self.requests:
                return "No S3FS requests captured yet."
            
            df = pd.DataFrame(self.requests)
            
            # Calculate elapsed time (time since last request)
            if len(df) > 1:
                # Sort by timestamp first to ensure correct calculation
                df = df.sort_values('timestamp')
                
                # Calculate time differences
                df['elapsed_ms'] = df['timestamp'].diff().dt.total_seconds() * 1000
                
                # Shift the elapsed times up by one row
                # This means elapsed_ms[i] is the time between timestamps[i-1] and timestamps[i]
                df['elapsed_ms'] = df['elapsed_ms'].shift(-1)
                
                # Fill NaN for the last row (there's no next request to calculate elapsed time to)
                df['elapsed_ms'] = df['elapsed_ms'].fillna(0)
            else:
                df['elapsed_ms'] = 0
                
            df['elapsed_ms'] = df['elapsed_ms'].apply(lambda x: round(x, 1))
            
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