#!/usr/bin/env python3
"""
P2P File Synchronization Script

Usage:
    On Peer 1: python p2p_sync.py --local-port 8001 --remote-port 8002 --dir ./shared_dir1
    On Peer 2: python p2p_sync.py --local-port 8002 --remote-port 8001 --dir ./shared_dir2
"""

import os
import time
import socket
import hashlib
import argparse
import threading
import json
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Constants
BUFFER_SIZE = 4096
SYNC_INTERVAL = 5  # seconds

class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, sync_manager):
        self.sync_manager = sync_manager
        self.last_modified = {}
        
    def on_any_event(self, event):
        if event.is_directory:
            return
            
        # Ignore hidden files, especially our .last_sync file
        if os.path.basename(event.src_path).startswith('.'):
            return
            
        # Ignore temporary files
        if event.src_path.endswith('.tmp'):
            return
            
        # Debounce to avoid duplicate events
        path = event.src_path
        current_time = time.time()
        
        if path in self.last_modified and current_time - self.last_modified[path] < 1:
            return
            
        self.last_modified[path] = current_time
        
        # Log file change
        rel_path = os.path.relpath(path, self.sync_manager.directory)
        print(f"File change detected: {rel_path} - Event type: {event.event_type}")
        
        # Schedule sync after a short delay to let file operations complete
        threading.Timer(0.5, self.sync_manager.schedule_sync).start()

class SyncManager:
    def __init__(self, directory, local_port, remote_host, remote_port):
        self.directory = os.path.abspath(directory)
        self.local_port = local_port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.sync_scheduled = False
        self.sync_lock = threading.Lock()
        self.file_index = {}  # path -> (size, mtime, md5)
        
        # Create directory if it doesn't exist
        os.makedirs(self.directory, exist_ok=True)
        
        # Initialize server
        self.server_thread = threading.Thread(target=self.run_server)
        self.server_thread.daemon = True
        self.server_thread.start()
        
        # Initialize periodic sync
        self.sync_thread = threading.Thread(target=self.periodic_sync)
        self.sync_thread.daemon = True
        self.sync_thread.start()
        
        # Initialize file watcher
        self.event_handler = FileChangeHandler(self)
        self.observer = Observer()
        self.observer.schedule(self.event_handler, self.directory, recursive=True)
        self.observer.start()
        
        # Create initial file index
        self.update_file_index()
    
    def relative_path(self, path):
        """Convert absolute path to relative path from the sync directory"""
        return os.path.relpath(path, self.directory)
    
    def absolute_path(self, rel_path):
        """Convert relative path to absolute path"""
        return os.path.join(self.directory, rel_path)
    
    def get_file_hash(self, path):
        """Calculate MD5 hash of a file"""
        hash_md5 = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def update_file_index(self):
        """Create an index of all files in the directory with their metadata"""
        new_index = {}
        
        for root, _, files in os.walk(self.directory):
            for filename in files:
                full_path = os.path.join(root, filename)
                rel_path = self.relative_path(full_path)
                
                try:
                    stat = os.stat(full_path)
                    # Only calculate hash if the file is new or changed
                    if (rel_path not in self.file_index or 
                        self.file_index[rel_path][0] != stat.st_size or 
                        self.file_index[rel_path][1] != stat.st_mtime):
                        file_hash = self.get_file_hash(full_path)
                    else:
                        file_hash = self.file_index[rel_path][2]
                        
                    new_index[rel_path] = (stat.st_size, stat.st_mtime, file_hash)
                except (FileNotFoundError, PermissionError) as e:
                    print(f"Error indexing {full_path}: {e}")
        
        self.file_index = new_index
        return new_index
    
    def run_server(self):
        """Run a server to handle incoming sync requests"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('0.0.0.0', self.local_port))
        server_socket.listen(5)
        
        print(f"Server listening on port {self.local_port}")
        
        while True:
            try:
                client_socket, addr = server_socket.accept()
                print(f"Connection from {addr}")
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
                client_thread.daemon = True
                client_thread.start()
            except Exception as e:
                print(f"Server error: {e}")
    
    def handle_client(self, client_socket):
        """Handle incoming client requests"""
        try:
            # Receive command
            data = client_socket.recv(BUFFER_SIZE).decode('utf-8')
            if not data:
                return
                
            command = json.loads(data)
            
            if command['action'] == 'get_index':
                # Send file index
                index = self.update_file_index()
                client_socket.sendall(json.dumps(index).encode('utf-8'))
                
            elif command['action'] == 'get_file':
                # Send requested file
                rel_path = command['path']
                abs_path = self.absolute_path(rel_path)
                
                if os.path.exists(abs_path) and os.path.isfile(abs_path):
                    # Send file size first
                    size = os.path.getsize(abs_path)
                    client_socket.sendall(str(size).encode('utf-8'))
                    
                    # Wait for acknowledgment
                    client_socket.recv(BUFFER_SIZE)
                    
                    # Send file content
                    with open(abs_path, 'rb') as f:
                        while True:
                            chunk = f.read(BUFFER_SIZE)
                            if not chunk:
                                break
                            client_socket.sendall(chunk)
                else:
                    client_socket.sendall(b"0")  # File doesn't exist
        
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            client_socket.close()
    
    def get_remote_index(self):
        """Get file index from remote peer"""
        try:
            # Connect to remote peer
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((self.remote_host, self.remote_port))
            
            # Request file index
            request = {'action': 'get_index'}
            client_socket.sendall(json.dumps(request).encode('utf-8'))
            
            # Receive file index
            data = client_socket.recv(BUFFER_SIZE * 10).decode('utf-8')  # Allow for large index
            remote_index = json.loads(data)
            
            client_socket.close()
            return remote_index
        except Exception as e:
            print(f"Error getting remote index: {e}")
            return {}
    
    def get_remote_file(self, rel_path):
        """Download file from remote peer"""
        try:
            # Connect to remote peer
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.settimeout(10)  # Add timeout to prevent hanging
            client_socket.connect((self.remote_host, self.remote_port))
            
            # Request file
            request = {'action': 'get_file', 'path': rel_path}
            client_socket.sendall(json.dumps(request).encode('utf-8'))
            
            # Get file size
            size_data = client_socket.recv(BUFFER_SIZE).decode('utf-8')
            if size_data == "0":
                print(f"File {rel_path} not found on remote peer")
                client_socket.close()
                return False
                
            try:
                file_size = int(size_data)
            except ValueError:
                print(f"Invalid file size received for {rel_path}: {size_data}")
                client_socket.close()
                return False
                
            # Send acknowledgment
            client_socket.sendall(b"ACK")
            
            # Create directory if needed
            abs_path = self.absolute_path(rel_path)
            os.makedirs(os.path.dirname(abs_path), exist_ok=True)
            
            # Write to a temporary file first
            temp_path = abs_path + ".tmp"
            received = 0
            
            with open(temp_path, 'wb') as f:
                while received < file_size:
                    try:
                        chunk = client_socket.recv(min(BUFFER_SIZE, file_size - received))
                        if not chunk:
                            break
                        f.write(chunk)
                        received += len(chunk)
                    except socket.timeout:
                        print(f"Timeout while downloading {rel_path}")
                        client_socket.close()
                        os.remove(temp_path)
                        return False
            
            client_socket.close()
            
            # Check if we received the complete file
            if received < file_size:
                print(f"Incomplete download of {rel_path}: got {received} of {file_size} bytes")
                os.remove(temp_path)
                return False
                
            # Replace original file with temporary file
            shutil.move(temp_path, abs_path)
            
            print(f"Downloaded {rel_path} ({file_size} bytes)")
            return True
        except Exception as e:
            print(f"Error downloading file {rel_path}: {e}")
            # Clean up temp file if it exists
            temp_path = self.absolute_path(rel_path) + ".tmp"
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            return False
    
    def schedule_sync(self):
        """Schedule a sync to happen soon"""
        with self.sync_lock:
            if not self.sync_scheduled:
                self.sync_scheduled = True
                threading.Timer(1, self.sync_now).start()
    
    def sync_now(self):
        """Perform synchronization with remote peer"""
        with self.sync_lock:
            self.sync_scheduled = False
            
        try:
            print("Syncing with remote peer...")
            
            # Update local index
            local_index = self.update_file_index()
            
            # Get remote index
            remote_index = self.get_remote_index()
            
            # Since this is a bidirectional sync, don't delete files that only exist on one side
            # Instead, propagate them to the other side
            
            # Keep track of last sync time for this file to resolve conflicts
            current_time = time.time()
            last_sync_file = os.path.join(self.directory, '.last_sync')
            
            if os.path.exists(last_sync_file):
                try:
                    with open(last_sync_file, 'r') as f:
                        last_sync_time = float(f.read().strip())
                except (ValueError, IOError):
                    last_sync_time = 0
            else:
                last_sync_time = 0
            
            # Find files to download (files that are newer on remote or don't exist locally)
            to_download = []
            
            for rel_path, (remote_size, remote_mtime, remote_hash) in remote_index.items():
                if rel_path not in local_index:
                    # File doesn't exist locally, download it
                    to_download.append(rel_path)
                elif local_index[rel_path][2] != remote_hash:
                    # File hash is different, compare modification times to resolve conflict
                    local_mtime = local_index[rel_path][1]
                    if remote_mtime > local_mtime:
                        # Remote file is newer
                        to_download.append(rel_path)
            
            # Download newer files
            for rel_path in to_download:
                self.get_remote_file(rel_path)
            
            # Only delete files if we're sure they were deleted on the remote side
            # and not newly created locally since the last sync
            if last_sync_time > 0:
                to_delete = []
                
                for rel_path, (local_size, local_mtime, local_hash) in local_index.items():
                    # Only consider files that existed before this sync cycle
                    if local_mtime < last_sync_time and rel_path not in remote_index:
                        # File exists locally but not on remote and it's not a new local file
                        to_delete.append(rel_path)
                
                # Delete files that were truly deleted on the remote side
                for rel_path in to_delete:
                    abs_path = self.absolute_path(rel_path)
                    try:
                        os.remove(abs_path)
                        print(f"Deleted {rel_path}")
                    except OSError as e:
                        print(f"Error deleting {rel_path}: {e}")
                
                print(f"Sync completed. Downloaded {len(to_download)} files, deleted {len(to_delete)} files.")
            else:
                print(f"Initial sync completed. Downloaded {len(to_download)} files.")
            
            # Update last sync time
            with open(last_sync_file, 'w') as f:
                f.write(str(current_time))
            
            # Update index after sync
            self.update_file_index()
            
        except Exception as e:
            print(f"Sync error: {e}")
    
    def periodic_sync(self):
        """Perform periodic synchronization"""
        while True:
            time.sleep(SYNC_INTERVAL)
            self.sync_now()

def main():
    parser = argparse.ArgumentParser(description='P2P File Synchronization')
    parser.add_argument('--local-port', type=int, required=True, help='Local port to listen on')
    parser.add_argument('--remote-port', type=int, required=True, help='Remote port to connect to')
    parser.add_argument('--remote-host', type=str, default='localhost', help='Remote host to connect to')
    parser.add_argument('--dir', type=str, required=True, help='Directory to synchronize')
    parser.add_argument('--initial-sync-delay', type=int, default=3, help='Delay in seconds before initial sync')
    
    args = parser.parse_args()
    
    print(f"""
P2P File Synchronization
------------------------
Local directory: {args.dir}
Local port: {args.local_port}
Remote host: {args.remote_host}
Remote port: {args.remote_port}
Initial sync delay: {args.initial_sync_delay} seconds
""")
    
    # Create the sync manager
    sync_manager = SyncManager(args.dir, args.local_port, args.remote_host, args.remote_port)
    
    # Wait a bit before initial sync to ensure both peers are up
    print(f"Waiting {args.initial_sync_delay} seconds before initial sync...")
    time.sleep(args.initial_sync_delay)
    sync_manager.sync_now()
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")

if __name__ == "__main__":
    main()