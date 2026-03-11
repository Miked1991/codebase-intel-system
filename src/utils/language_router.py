"""Language router for tree-sitter grammar selection."""

import os
from pathlib import Path
from typing import Optional, Dict, Any, List
import tree_sitter
import tree_sitter_python
import tree_sitter_sql
import tree_sitter_yaml
import tree_sitter_javascript
import tree_sitter_typescript


class LanguageRouter:
    """Routes files to appropriate tree-sitter grammars based on extension."""
    
    # Language configurations with proper grammar loading
    LANGUAGE_CONFIGS = {
        '.py': {
            'name': 'python',
            'grammar': tree_sitter_python.language(),
            'parser': None,
            'query_files': ['python']
        },
        '.sql': {
            'name': 'sql',
            'grammar': tree_sitter_sql.language(),
            'parser': None,
            'query_files': ['sql']
        },
        '.yml': {
            'name': 'yaml',
            'grammar': tree_sitter_yaml.language(),
            'parser': None,
            'query_files': ['yaml']
        },
        '.yaml': {
            'name': 'yaml',
            'grammar': tree_sitter_yaml.language(),
            'parser': None,
            'query_files': ['yaml']
        },
        '.js': {
            'name': 'javascript',
            'grammar': tree_sitter_javascript.language(),
            'parser': None,
            'query_files': ['javascript']
        },
        '.jsx': {
            'name': 'javascript',
            'grammar': tree_sitter_javascript.language(),
            'parser': None,
            'query_files': ['javascript']
        },
        '.ts': {
            'name': 'typescript',
            'grammar': tree_sitter_typescript.language_typescript(),
            'parser': None,
            'query_files': ['typescript']
        },
        '.tsx': {
            'name': 'typescript',
            'grammar': tree_sitter_typescript.language_tsx(),
            'parser': None,
            'query_files': ['typescript']
        },
        '.ipynb': {
            'name': 'jupyter',
            'grammar': None,
            'parser': None,
            'query_files': []
        },
        '.md': {
            'name': 'markdown',
            'grammar': None,  # Would need tree-sitter-markdown
            'parser': None,
            'query_files': []
        },
        '.json': {
            'name': 'json',
            'grammar': None,  # Would need tree-sitter-json
            'parser': None,
            'query_files': []
        }
    }
    
    # Language-specific query patterns
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
                    name: (identifier) @func_name
                    parameters: (parameters) @params
                    return_type: (type)? @return_type
                    body: (block) @body) @func_def
            """,
            'classes': """
                (class_definition
                    name: (identifier) @class_name
                    superclasses: (argument_list)? @superclasses
                    body: (block) @body) @class_def
            """,
            'decorators': """
                (decorator
                    (identifier) @decorator_name)
            """,
            'docstrings': """
                (module (expression_statement (string)) @module_docstring)
                (function_definition
                    body: (block (expression_statement (string)) @func_docstring))
                (class_definition
                    body: (block (expression_statement (string)) @class_docstring))
            """,
            'calls': """
                (call
                    function: (attribute
                        object: (identifier) @obj
                        attribute: (identifier) @method)
                    arguments: (argument_list) @args)
            """
        },
        'sql': {
            'tables': """
                (select
                    (from (table_ref (identifier)) @table_name))
                (insert
                    (into (table_ref (identifier)) @target_table))
                (update
                    (update_set (table_ref (identifier)) @target_table))
                (delete
                    (from (table_ref (identifier)) @target_table))
                (join
                    (table_ref (identifier)) @joined_table)
            """,
            'ctes': """
                (with
                    (common_table_expression
                        name: (identifier) @cte_name
                        query: (select) @cte_query))
            """,
            'functions': """
                (function_call
                    name: (identifier) @func_name
                    arguments: (argument_list) @func_args)
            """
        },
        'yaml': {
            'keys': """
                (block_mapping_pair
                    key: (flow_node) @key
                    value: (flow_node) @value)
            """,
            'sequences': """
                (block_sequence
                    (block_sequence_item (flow_node) @item))
            """
        },
        'javascript': {
            'imports': """
                (import_statement
                    source: (string) @source)
                (import_specifier
                    imported: (identifier) @imported
                    local: (identifier) @local)
            """,
            'exports': """
                (export_statement
                    value: (identifier) @exported)
            """,
            'functions': """
                (function_declaration
                    name: (identifier) @func_name)
                (arrow_function) @arrow_func
            """
        },
        'typescript': {
            'imports': """
                (import_statement
                    source: (string) @source)
                (import_specifier
                    imported: (identifier) @imported
                    local: (identifier) @local)
            """,
            'exports': """
                (export_statement
                    value: (identifier) @exported)
            """,
            'functions': """
                (function_declaration
                    name: (identifier) @func_name)
                (arrow_function) @arrow_func
            """,
            'interfaces': """
                (interface_declaration
                    name: (identifier) @interface_name)
            """,
            'types': """
                (type_alias_declaration
                    name: (identifier) @type_name)
            """
        }
    }
    
    def __init__(self):
        """Initialize parsers for each language."""
        self.parsers = {}
        self.languages = {}
        self.compiled_queries = {}
        self._init_parsers()
    
    def _init_parsers(self):
        """Create parsers for each language with proper grammar initialization."""
        for ext, config in self.LANGUAGE_CONFIGS.items():
            if config['grammar']:
                try:
                    # Create parser and set language
                    parser = tree_sitter.Parser()
                    parser.set_language(config['grammar'])
                    
                    # Store both parser and language
                    self.parsers[ext] = parser
                    self.languages[config['name']] = config['grammar']
                    
                    # Pre-compile queries for this language
                    self._compile_queries_for_language(config['name'], config['grammar'])
                    
                except Exception as e:
                    print(f"Warning: Could not initialize parser for {ext}: {e}")
    
    def _compile_queries_for_language(self, language_name: str, grammar):
        """Pre-compile all queries for a language."""
        self.compiled_queries[language_name] = {}
        
        if language_name in self.QUERIES:
            for query_name, query_string in self.QUERIES[language_name].items():
                try:
                    compiled_query = grammar.query(query_string)
                    self.compiled_queries[language_name][query_name] = compiled_query
                except Exception as e:
                    print(f"Warning: Could not compile query '{query_name}' for {language_name}: {e}")
    
    def get_parser(self, file_path: str) -> Optional[tree_sitter.Parser]:
        """
        Get the appropriate parser for a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            tree_sitter.Parser instance or None if not supported
        """
        ext = self._get_extension(file_path)
        
        # Handle Jupyter notebooks specially
        if ext == '.ipynb':
            return None
        
        return self.parsers.get(ext)
    
    def get_language(self, file_path: str) -> Optional[str]:
        """
        Get the language name for a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Language name (e.g., 'python', 'sql') or None if not supported
        """
        ext = self._get_extension(file_path)
        config = self.LANGUAGE_CONFIGS.get(ext, {})
        return config.get('name')
    
    def get_grammar(self, language_name: str) -> Optional[Any]:
        """
        Get the grammar object for a language.
        
        Args:
            language_name: Name of the language
            
        Returns:
            Grammar object or None if not found
        """
        return self.languages.get(language_name)
    
    def get_query(self, language_name: str, query_name: str) -> Optional[Any]:
        """
        Get a compiled query for a specific language and query type.
        
        Args:
            language_name: Name of the language
            query_name: Name of the query (e.g., 'imports', 'functions')
            
        Returns:
            Compiled query object or None if not found
        """
        if language_name in self.compiled_queries:
            return self.compiled_queries[language_name].get(query_name)
        return None
    
    def parse_file(self, file_path: str, content: bytes) -> Optional[tree_sitter.Tree]:
        """
        Parse a file and return its AST.
        
        Args:
            file_path: Path to the file
            content: File content as bytes
            
        Returns:
            tree_sitter.Tree object or None if parsing fails
        """
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
        Run a specific query on a file.
        
        Args:
            file_path: Path to the file
            query_name: Name of the query to run
            
        Returns:
            List of captured nodes with their text and positions
        """
        language = self.get_language(file_path)
        if not language:
            return []
        
        query = self.get_query(language, query_name)
        if not query:
            return []
        
        # Read and parse file
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
            
            tree = self.parse_file(file_path, content)
            if not tree:
                return []
            
            # Execute query
            captures = query.captures(tree.root_node)
            
            # Process captures
            results = []
            for node, tag in captures.items():
                if node.text:
                    results.append({
                        'tag': tag,
                        'text': node.text.decode('utf8'),
                        'start_point': node.start_point,
                        'end_point': node.end_point,
                        'start_byte': node.start_byte,
                        'end_byte': node.end_byte
                    })
            
            return results
            
        except Exception as e:
            print(f"Error querying {file_path}: {e}")
            return []
    
    def is_supported(self, file_path: str) -> bool:
        """
        Check if a file type is supported.
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if the file type is supported
        """
        ext = self._get_extension(file_path)
        return ext in self.LANGUAGE_CONFIGS
    
    def get_supported_extensions(self) -> List[str]:
        """Get list of supported file extensions."""
        return list(self.LANGUAGE_CONFIGS.keys())
    
    def get_supported_languages(self) -> List[str]:
        """Get list of supported language names."""
        languages = set()
        for config in self.LANGUAGE_CONFIGS.values():
            if config['name']:
                languages.add(config['name'])
        return sorted(list(languages))
    
    def _get_extension(self, file_path: str) -> str:
        """
        Get the extension of a file, handling double extensions like .spec.ts.
        
        Args:
            file_path: Path to the file
            
        Returns:
            File extension with dot
        """
        path = Path(file_path)
        
        # Check for common double extensions
        double_extensions = ['.spec.ts', '.test.ts', '.d.ts', '.spec.js', '.test.js']
        
        for double_ext in double_extensions:
            if str(path).endswith(double_ext):
                return double_ext
        
        # Return normal extension
        return path.suffix.lower()
    
    def get_file_type_description(self, file_path: str) -> str:
        """
        Get a human-readable description of the file type.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Description of the file type
        """
        language = self.get_language(file_path)
        ext = self._get_extension(file_path)
        
        descriptions = {
            '.py': 'Python source file',
            '.sql': 'SQL query file',
            '.yml': 'YAML configuration',
            '.yaml': 'YAML configuration',
            '.js': 'JavaScript source file',
            '.jsx': 'JavaScript React component',
            '.ts': 'TypeScript source file',
            '.tsx': 'TypeScript React component',
            '.ipynb': 'Jupyter notebook',
            '.md': 'Markdown documentation',
            '.json': 'JSON data file'
        }
        
        base_desc = descriptions.get(ext, f'Unknown file type ({ext})')
        
        if language:
            return f"{base_desc} ({language})"
        
        return base_desc


# Singleton instance for global use
_default_router = None


def get_language_router() -> LanguageRouter:
    """Get or create the default language router instance."""
    global _default_router
    if _default_router is None:
        _default_router = LanguageRouter()
    return _default_router