"""Language router for tree-sitter grammar selection - Fixed for 0.22.0+ API."""

import os
from pathlib import Path
from typing import Optional, Dict, Any, List
from tree_sitter import Language, Parser, Query, Tree, Node

# Import language grammars
try:
    import tree_sitter_python
    HAS_PYTHON = True
except ImportError:
    HAS_PYTHON = False
    print("⚠️  tree-sitter-python not installed. Python parsing will be limited.")

try:
    import tree_sitter_sql
    HAS_SQL = True
except ImportError:
    HAS_SQL = False
    print("⚠️  tree-sitter-sql not installed. SQL parsing will be limited.")

try:
    import tree_sitter_yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False
    print("⚠️  tree-sitter-yaml not installed. YAML parsing will be limited.")

try:
    import tree_sitter_javascript
    HAS_JAVASCRIPT = True
except ImportError:
    HAS_JAVASCRIPT = False
    print("⚠️  tree-sitter-javascript not installed. JavaScript parsing will be limited.")

try:
    import tree_sitter_typescript
    HAS_TYPESCRIPT = True
except ImportError:
    HAS_TYPESCRIPT = False
    print("⚠️  tree-sitter-typescript not installed. TypeScript parsing will be limited.")


class LanguageRouter:
    """Routes files to appropriate tree-sitter grammars based on extension."""
    
    # Language configurations
    LANGUAGE_CONFIGS = {
        '.py': {
            'name': 'python',
            'module': tree_sitter_python if HAS_PYTHON else None,
            'language_func': lambda m: m.language() if hasattr(m, 'language') else None,
        },
        '.sql': {
            'name': 'sql',
            'module': tree_sitter_sql if HAS_SQL else None,
            'language_func': lambda m: m.language() if hasattr(m, 'language') else None,
        },
        '.yml': {
            'name': 'yaml',
            'module': tree_sitter_yaml if HAS_YAML else None,
            'language_func': lambda m: m.language() if hasattr(m, 'language') else None,
        },
        '.yaml': {
            'name': 'yaml',
            'module': tree_sitter_yaml if HAS_YAML else None,
            'language_func': lambda m: m.language() if hasattr(m, 'language') else None,
        },
        '.js': {
            'name': 'javascript',
            'module': tree_sitter_javascript if HAS_JAVASCRIPT else None,
            'language_func': lambda m: m.language() if hasattr(m, 'language') else None,
        },
        '.jsx': {
            'name': 'javascript',
            'module': tree_sitter_javascript if HAS_JAVASCRIPT else None,
            'language_func': lambda m: m.language() if hasattr(m, 'language') else None,
        },
        '.ts': {
            'name': 'typescript',
            'module': tree_sitter_typescript if HAS_TYPESCRIPT else None,
            'language_func': lambda m: m.language_typescript() if hasattr(m, 'language_typescript') else None,
        },
        '.tsx': {
            'name': 'typescript',
            'module': tree_sitter_typescript if HAS_TYPESCRIPT else None,
            'language_func': lambda m: m.language_tsx() if hasattr(m, 'language_tsx') else None,
        }
    }
    
    # Simplified queries
    QUERIES = {
        'python': {
            'imports': """
                (import_statement
                    name: (dotted_name) @import_name)
                (import_from_statement
                    module_name: (dotted_name) @from_module
                    name: (dotted_name) @import_name)
            """,
            'functions': """
                (function_definition
                    name: (identifier) @func_name) @func_def
            """,
            'classes': """
                (class_definition
                    name: (identifier) @class_name) @class_def
            """
        },
        'sql': {
            'tables': """
                (select
                    (from (identifier) @table_name))
                (insert
                    (into (identifier) @target_table))
            """
        },
        'yaml': {
            'keys': """
                (block_mapping_pair
                    key: (flow_node) @key)
            """
        },
        'javascript': {
            'imports': """
                (import_statement
                    source: (string) @source)
            """,
            'functions': """
                (function_declaration
                    name: (identifier) @func_name)
            """
        },
        'typescript': {
            'imports': """
                (import_statement
                    source: (string) @source)
            """,
            'functions': """
                (function_declaration
                    name: (identifier) @func_name)
            """
        }
    }
    
    def __init__(self):
        """Initialize parsers for each language."""
        self.parsers: Dict[str, Parser] = {}
        self.languages: Dict[str, Language] = {}
        self.compiled_queries: Dict[str, Dict[str, Query]] = {}
        self._init_parsers()
        print(f"✅ LanguageRouter initialized with {len(self.parsers)} parsers")
    
    def _init_parsers(self):
        """Create parsers for each language."""
        for ext, config in self.LANGUAGE_CONFIGS.items():
            if config['module'] and config['language_func']:
                try:
                    # Get language object
                    lang_obj = config['language_func'](config['module'])
                    if lang_obj is None:
                        continue
                    
                    # Wrap in Language
                    language = Language(lang_obj)
                    
                    # Create parser with language
                    parser = Parser(language)
                    
                    # Store
                    self.parsers[ext] = parser
                    self.languages[config['name']] = language
                    
                    # Compile queries
                    self._compile_queries(config['name'], language)
                    
                except Exception as e:
                    print(f"Warning: Could not initialize parser for {ext}: {e}")
    
    def _compile_queries(self, language_name: str, language: Language):
        """Compile queries for a language."""
        self.compiled_queries[language_name] = {}
        
        if language_name in self.QUERIES:
            for query_name, query_string in self.QUERIES[language_name].items():
                try:
                    query = language.query(query_string)
                    self.compiled_queries[language_name][query_name] = query
                except Exception as e:
                    print(f"Warning: Could not compile query '{query_name}' for {language_name}: {e}")
    
    def get_parser(self, file_path: str) -> Optional[Parser]:
        """Get parser for file."""
        ext = self._get_extension(file_path)
        return self.parsers.get(ext)
    
    def get_language_name(self, file_path: str) -> Optional[str]:
        """Get language name for file."""
        ext = self._get_extension(file_path)
        config = self.LANGUAGE_CONFIGS.get(ext, {})
        return config.get('name')
    
    def get_query(self, language_name: str, query_name: str) -> Optional[Query]:
        """Get compiled query."""
        if language_name in self.compiled_queries:
            return self.compiled_queries[language_name].get(query_name)
        return None
    
    def parse_file(self, file_path: str, content: bytes) -> Optional[Tree]:
        """Parse file and return AST."""
        parser = self.get_parser(file_path)
        if not parser:
            return None
        
        try:
            tree = parser.parse(content)
            return tree
        except Exception as e:
            print(f"Error parsing {file_path}: {e}")
            return None
    
    def query_file(self, file_path: str, query_name: str) -> List[Dict[str, Any]]:
        """
        Run a query on a file using tree-sitter 0.22.0+ API.
        
        FIXED: Using query.matches() instead of QueryCursor
        """
        language_name = self.get_language_name(file_path)
        if not language_name:
            return []
        
        query = self.get_query(language_name, query_name)
        if not query:
            return []
        
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
            
            tree = self.parse_file(file_path, content)
            if not tree:
                return []
            
            results = []
            
            # NEW API for tree-sitter 0.22.0+
            # Query objects now have a matches() method
            matches = query.matches(tree.root_node)
            
            # Process matches
            for match in matches:
                # Each match has pattern and captures
                for capture in match.captures:
                    node = capture.node
                    tag = capture.tag if hasattr(capture, 'tag') else capture.name
                    
                    if node and node.text:
                        try:
                            text = node.text.decode('utf-8', errors='ignore')
                        except:
                            text = ""
                        
                        results.append({
                            'tag': tag,
                            'text': text,
                            'start_line': node.start_point[0] + 1,
                            'end_line': node.end_point[0] + 1
                        })
            
            return results
            
        except Exception as e:
            print(f"Error querying {file_path}: {e}")
            return []
    
    def query_tree(self, tree: Tree, language_name: str, query_name: str) -> List[Dict[str, Any]]:
        """Run query on an existing tree using 0.22.0+ API."""
        query = self.get_query(language_name, query_name)
        if not query:
            return []
        
        results = []
        
        # NEW API: query.matches()
        matches = query.matches(tree.root_node)
        
        for match in matches:
            for capture in match.captures:
                node = capture.node
                tag = capture.tag if hasattr(capture, 'tag') else capture.name
                
                if node and node.text:
                    try:
                        text = node.text.decode('utf-8', errors='ignore')
                    except:
                        text = ""
                    
                    results.append({
                        'tag': tag,
                        'text': text,
                        'start_line': node.start_point[0] + 1,
                        'end_line': node.end_point[0] + 1
                    })
        
        return results
    
    def is_supported(self, file_path: str) -> bool:
        """Check if file type is supported."""
        ext = self._get_extension(file_path)
        return ext in self.parsers
    
    def _get_extension(self, file_path: str) -> str:
        """Get file extension."""
        path = Path(file_path)
        return path.suffix.lower()


# Singleton instance
_default_router = None


def get_language_router() -> LanguageRouter:
    """Get or create the default language router instance."""
    global _default_router
    if _default_router is None:
        _default_router = LanguageRouter()
    return _default_router