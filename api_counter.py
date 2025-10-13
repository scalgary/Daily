# api_counter.py
"""
Module for tracking API usage with persistent storage
"""

import json
from functools import wraps
from pathlib import Path


class PersistentAPICounter:
    """Counter that persists to JSON file"""
    
    def __init__(self, file='api_usage.json'):
        self.file = Path(file)
        self.usage = self._load_or_create()
    
    def _load_or_create(self):
        """Load existing file or create new one"""
        if self.file.exists():
            try:
                with open(self.file, 'r') as f:
                    data = json.load(f)
                    print(f"✓ Loaded existing usage from {self.file}")
                    print(f"  Current counts: {data}")
                    return data
            except Exception as e:
                print(f"⚠️ Error reading {self.file}: {e}")
                print(f"  Creating new file...")
                self._create_new_file()
                return {}
        else:
            print(f"📝 File {self.file} not found, creating new one...")
            self._create_new_file()
            return {}
    
    def _create_new_file(self):
        """Create new empty JSON file"""
        with open(self.file, 'w') as f:
            json.dump({}, f, indent=2)
        print(f"✓ Created {self.file}")
    
    def _save(self):
        """Save current usage to file"""
        with open(self.file, 'w') as f:
            json.dump(self.usage, f, indent=2)
    
    def count(self, api_name):
        """Increment counter for API and save"""
        self.usage[api_name] = self.usage.get(api_name, 0) + 1
        self._save()
        return self.usage[api_name]
    
    def get_count(self, api_name):
        """Get current count for an API"""
        return self.usage.get(api_name, 0)
    
    def show(self):
        """Display all usage"""
        print("\n" + "="*40)
        print("API USAGE SUMMARY")
        print("="*40)
        if not self.usage:
            print("No API calls tracked yet")
        else:
            total = sum(self.usage.values())
            for api_name, count in sorted(self.usage.items()):
                print(f"  {api_name}: {count} calls")
            print("-"*40)
            print(f"  TOTAL: {total} calls")
        print("="*40 + "\n")
    
    def reset(self):
        """Reset all counters"""
        self.usage = {}
        self._save()
        print("✓ All counters reset")


def count_api_decorator(counter):
    """
    Factory function to create a decorator with a specific counter instance
    
    Usage:
        counter = PersistentAPICounter('my_api.json')
        count_api = count_api_decorator(counter)
        
        @count_api('API_NAME')
        def my_function():
            pass
    """
    def count_api(api_name):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                result = func(*args, **kwargs)
                count = counter.count(api_name)
                #print(f"✓ {api_name} called → Total: {count}")
                return result
            return wrapper
        return decorator
    return count_api