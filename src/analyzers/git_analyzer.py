"""Git analysis utilities."""

import subprocess
from pathlib import Path
from typing import Dict, List
from datetime import datetime, timedelta


class GitAnalyzer:
    """Analyzes git history for change velocity."""
    
    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path)
    
    def get_change_velocity(self, days: int = 30) -> Dict[str, int]:
        """Get change count per file in the last N days."""
        since_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        try:
            # Run git log command
            result = subprocess.run(
                ['git', 'log', f'--since={since_date}', '--name-only', '--pretty=format:'],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                check=True
            )
            
            # Count file changes
            changes = {}
            for line in result.stdout.split('\n'):
                line = line.strip()
                if line and not line.startswith('{'):
                    changes[line] = changes.get(line, 0) + 1
            
            return changes
            
        except subprocess.CalledProcessError:
            return {}
        except FileNotFoundError:
            # Not a git repository
            return {}
    
    def get_current_commit(self) -> str:
        """Get the current commit hash."""
        try:
            result = subprocess.run(
                ['git', 'rev-parse', 'HEAD'],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.strip()
        except:
            return ""
    
    def get_changed_files(self, base_commit: str) -> List[str]:
        """Get files changed since base_commit."""
        try:
            result = subprocess.run(
                ['git', 'diff', '--name-only', base_commit],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                check=True
            )
            return [f for f in result.stdout.split('\n') if f]
        except:
            return []