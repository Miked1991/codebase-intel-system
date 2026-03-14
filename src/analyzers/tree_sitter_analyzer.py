"""Tree-sitter based code analysis for multiple languages."""

import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import tree_sitter

from ..utils.language_router import get_language_router


class TreeSitterAnalyzer:
    """Multi-language AST parser using tree-sitter."""
    
    def __init__(self):
        self.language_router = get_language_router()
        
        # Language-specific query patterns (maintained for reference)
        # Actual queries are now managed by the language router
        self.query_names = {
            'python': ['imports', 'functions', 'classes', 'decorators', 'docstrings', 'calls'],
            'sql': ['tables', 'ctes', 'functions'],
            'yaml': ['keys', 'sequences'],
            'javascript': ['imports', 'exports', 'functions'],
            'typescript': ['imports', 'exports', 'functions', 'interfaces', 'types']
        }
    
    def analyze_file(self, file_path: str) -> Dict[str, Any]:
        """Analyze a single file and return extracted information."""
        file_path = Path(file_path)
        
        if not file_path.exists():
            return {"error": f"File not found: {file_path}"}
        
        # Get language
        language = self.language_router.get_language(str(file_path))
        if not language:
            return {"error": f"Unsupported language for {file_path}"}
        
        # Read file with proper encoding handling
        try:
            # Always read as binary for tree-sitter
            with open(file_path, 'rb') as f:
                content = f.read()
        except Exception as e:
            return {"error": f"Error reading file: {e}"}
        
        # Parse with tree-sitter
        tree = self.language_router.parse_file(str(file_path), content)
        if not tree:
            return {"error": f"Failed to parse {file_path}"}
        
        # Analyze based on language
        result = {
            "language": language,
            "file": str(file_path),
            "file_type": self.language_router.get_file_type_description(str(file_path)),
            "loc": len(content.splitlines()),
            "size_bytes": len(content)
        }
        
        # Run all relevant queries for this language
        if language in self.query_names:
            for query_name in self.query_names[language]:
                try:
                    query_result = self._run_query(file_path, query_name)
                    if query_result:
                        result[query_name] = query_result
                except Exception as e:
                    print(f"Warning: Query '{query_name}' failed for {file_path}: {e}")
        
        # Calculate additional metrics
        result['comment_ratio'] = self._calculate_comment_ratio(content, language)
        result['has_errors'] = tree.root_node.has_error
        
        return result
    
    def _run_query(self, file_path: str, query_name: str) -> List[Dict[str, Any]]:
        """Run a specific query on a file."""
        return self.language_router.query_file(str(file_path), query_name)
    
    def _calculate_comment_ratio(self, content: bytes, language: str) -> float:
        """Calculate ratio of comments to code."""
        try:
            # Try to decode with utf-8, ignore errors
            try:
                text = content.decode('utf-8')
            except UnicodeDecodeError:
                # Fallback to latin-1 which never fails
                text = content.decode('latin-1')
            
            lines = text.split('\n')
            comment_lines = 0
            
            if language == 'python':
                in_multiline = False
                for line in lines:
                    stripped = line.strip()
                    if stripped.startswith('"""') or stripped.startswith("'''"):
                        in_multiline = not in_multiline
                        comment_lines += 1
                    elif in_multiline:
                        comment_lines += 1
                    elif stripped.startswith('#'):
                        comment_lines += 1
            
            elif language in ['javascript', 'typescript']:
                in_multiline = False
                for line in lines:
                    stripped = line.strip()
                    if '/*' in stripped and '*/' not in stripped:
                        in_multiline = True
                        comment_lines += 1
                    elif in_multiline:
                        comment_lines += 1
                        if '*/' in stripped:
                            in_multiline = False
                    elif stripped.startswith('//'):
                        comment_lines += 1
            
            elif language == 'sql':
                for line in lines:
                    stripped = line.strip()
                    if stripped.startswith('--') or stripped.startswith('/*'):
                        comment_lines += 1
            
            return comment_lines / max(len(lines), 1)
            
        except Exception:
            return 0.0
    
    def analyze_directory(self, directory_path: str, extensions: List[str] = None) -> Dict[str, Any]:
        """Analyze all supported files in a directory."""
        if extensions is None:
            extensions = self.language_router.get_supported_extensions()
        
        directory = Path(directory_path)
        results = {}
        
        # Count total files for progress
        total_files = 0
        for ext in extensions:
            total_files += len(list(directory.rglob(f"*{ext}")))
        
        processed = 0
        for ext in extensions:
            for file_path in directory.rglob(f"*{ext}"):
                if file_path.is_file():
                    rel_path = str(file_path.relative_to(directory))
                    results[rel_path] = self.analyze_file(str(file_path))
                    processed += 1
                    
                    # Print progress for large directories
                    if processed % 100 == 0:
                        print(f"Processed {processed}/{total_files} files...")
        
        return results
    
    def get_file_summary(self, file_path: str) -> Dict[str, Any]:
        """Get a quick summary of a file."""
        result = self.analyze_file(file_path)
        
        if 'error' in result:
            return result
        
        # Create summary
        summary = {
            "file": result['file'],
            "language": result['language'],
            "type": result['file_type'],
            "lines": result['loc'],
            "size": f"{result['size_bytes'] / 1024:.1f} KB"
        }
        
        # Add key metrics
        if 'functions' in result:
            summary['functions'] = len(result['functions'])
        if 'classes' in result:
            summary['classes'] = len(result['classes'])
        if 'imports' in result:
            summary['imports'] = len(result['imports'])
        
        return summary