"""Semanticist Agent - LLM-powered semantic analysis with rate limiting."""

import os
import time
import sys
import csv
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import threading
import re

# Fix for OverflowError: Use a large but safe value for CSV field size
# On Windows, the maximum is typically 2^31-1 (2147483647)
try:
    # Try with sys.maxsize first
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    # If that overflows, use a large but safe value (2 GB)
    # 2**31 - 1 is the maximum for 32-bit signed integers
    csv.field_size_limit(2_147_483_647)
except Exception:
    # Last resort - use a reasonable default
    csv.field_size_limit(134_217_728)  # 128 MB

from ..models.nodes import ModuleNode
from ..utils.context_window import ContextWindowBudget

# Load environment variables
from dotenv import load_dotenv
load_dotenv()


class RateLimiter:
    """Rate limiter for API calls to prevent hitting limits."""
    
    def __init__(self, calls_per_minute: int = 30):
        """
        Initialize rate limiter.
        
        Args:
            calls_per_minute: Maximum number of API calls per minute
        """
        self.calls_per_minute = calls_per_minute
        self.calls = []
        self.lock = threading.Lock()
        print(f"✅ RateLimiter initialized: {calls_per_minute} calls/minute")
    
    def wait_if_needed(self):
        """Wait if rate limit would be exceeded."""
        with self.lock:
            now = datetime.now()
            # Remove calls older than 1 minute
            self.calls = [t for t in self.calls if now - t < timedelta(minutes=1)]
            
            if len(self.calls) >= self.calls_per_minute:
                # Calculate wait time
                oldest = min(self.calls)
                wait_time = 60 - (now - oldest).seconds
                if wait_time > 0:
                    print(f"⏱️  Rate limit reached, waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                # Retry after waiting
                self.calls = [t for t in self.calls if now - t < timedelta(minutes=1)]
            
            self.calls.append(now)


class SemanticistAgent:
    """Agent responsible for LLM-powered semantic understanding with rate limiting."""
    
    def __init__(self, repo_path: str, groq_api_key: Optional[str] = None):
        self.repo_path = Path(repo_path)
        
        # Get API key from parameter or environment
        self.groq_api_key = groq_api_key or os.getenv('GROQ_API_KEY')
        
        # Validate API key
        self.api_key_valid = self._validate_api_key()
        self.invalid_key_detected = False
        
        # Initialize context budget
        self.context_budget = ContextWindowBudget(max_budget=1.0)  # $1 budget
        
        # Initialize rate limiter - 30 calls per minute is safe for most APIs
        self.rate_limiter = RateLimiter(calls_per_minute=30)
        
        # Initialize embedding model for clustering
        try:
            from sentence_transformers import SentenceTransformer
            self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
            print("✅ SentenceTransformer loaded successfully")
        except Exception as e:
            self.embedder = None
            print(f"⚠️  Could not load SentenceTransformer: {e}")
        
        # Cache for purpose statements
        self.purpose_cache: Dict[str, str] = {}
        self.doc_drift_flags: List[Dict[str, Any]] = []
        
        # Track API usage
        self.api_calls = 0
        self.api_errors = 0
        
        # Track disabled state
        self.disabled = False
        
        if not self.api_key_valid:
            print("⚠️  SemanticistAgent initialized with INVALID API key")
            print("    To fix: Get a valid API key from https://console.groq.com")
    
    def _validate_api_key(self) -> bool:
        """
        Validate the API key format and presence.
        
        Returns:
            True if API key appears valid, False otherwise
        """
        if not self.groq_api_key:
            print("❌ No GROQ_API_KEY found in environment or parameters")
            return False
        
        # Check if it's the placeholder
        if self.groq_api_key == 'your_actual_groq_api_key_here':
            print("❌ Invalid API Key: Using placeholder key")
            print("   Please get a real API key from https://console.groq.com")
            return False
        
        # Check if it's the example fake key
        if self.groq_api_key == 'gsk_your_actual_key_here':
            print("❌ Invalid API Key: Using example placeholder")
            return False
        
        # Check length (Groq API keys are typically long, starting with 'gsk_')
        if len(self.groq_api_key) < 20:
            print(f"⚠️  API key seems too short ({len(self.groq_api_key)} chars)")
            return False
        
        # Check if it starts with gsk_ (Groq API keys format)
        if not self.groq_api_key.startswith('gsk_'):
            print("⚠️  API key doesn't start with 'gsk_' - may be invalid")
        
        # Mask for display
        masked = self._mask_api_key(self.groq_api_key)
        print(f"✅ GROQ_API_KEY found: {masked}")
        
        return True
    
    def _mask_api_key(self, key: str) -> str:
        """Mask API key for safe display."""
        if len(key) > 8:
            return key[:4] + "*" * (len(key) - 8) + key[-4:]
        return "***too short***"
    
    def _is_rate_limit_error(self, error: Exception) -> bool:
        """Check if error is a rate limit error."""
        error_str = str(error).lower()
        rate_limit_phrases = [
            "rate limit", 
            "too many requests", 
            "429", 
            "rate_limit_exceeded",
            "quota exceeded"
        ]
        return any(phrase in error_str for phrase in rate_limit_phrases)
    
    def _is_auth_error(self, error: Exception) -> bool:
        """Check if error is an authentication error."""
        error_str = str(error).lower()
        auth_phrases = [
            "401", 
            "invalid api key", 
            "authentication", 
            "unauthorized",
            "invalid_api_key",
            "permission denied"
        ]
        return any(phrase in error_str for phrase in auth_phrases)
    
    def _safe_read_file(self, file_path: Path, max_size: int = 10_000_000) -> Optional[str]:
        """
        Safely read a file with proper error handling.
        
        Args:
            file_path: Path to file
            max_size: Maximum file size to read (default 10MB)
            
        Returns:
            File contents or None if error
        """
        try:
            # Check file size
            if file_path.stat().st_size > max_size:
                print(f"⚠️  File too large ({file_path.stat().st_size} bytes), skipping: {file_path}")
                return None
            
            # Try UTF-8 first
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            except UnicodeDecodeError:
                # Fallback to latin-1
                with open(file_path, 'r', encoding='latin-1') as f:
                    return f.read()
            except Exception as e:
                # If CSV field limit error, try binary read
                if "field larger than field limit" in str(e):
                    with open(file_path, 'rb') as f:
                        binary_content = f.read()
                    # Try to decode with error handling
                    return binary_content.decode('utf-8', errors='ignore')
                else:
                    raise
                    
        except Exception as e:
            print(f"⚠️  Error reading {file_path}: {e}")
            return None
    
    def analyze(self, modules: Dict[str, ModuleNode]) -> Dict[str, ModuleNode]:
        """Run semantic analysis on modules with rate limiting."""
        print("🧠 Semanticist: Analyzing module semantics")
        
        # Check if disabled
        if self.disabled:
            print("⚠️  Semanticist disabled due to previous errors")
            return modules
        
        # Check API key
        if not self.api_key_valid:
            print("⚠️  Invalid API key. Skipping LLM analysis.")
            print("    To fix: Get a valid API key from https://console.groq.com")
            print("    Then update your .env file with: GROQ_API_KEY=your_actual_key")
            return modules
        
        # Generate purpose statements for Python modules with rate limiting
        python_modules = [(path, module) for path, module in modules.items() 
                         if module.language == 'python']
        
        total = len(python_modules)
        print(f"  Found {total} Python modules to analyze")
        
        if total == 0:
            print("  No Python modules found - skipping semantic analysis")
            return modules
        
        # Limit to 20 modules for testing/development
        max_modules = min(20, total)
        print(f"  Processing first {max_modules} modules (limit set for development)...")
        
        processed = 0
        for i, (path, module) in enumerate(python_modules[:max_modules]):
            if i % 5 == 0 and i > 0:
                print(f"  Progress: {i}/{max_modules} modules processed")
            
            purpose = self._generate_purpose_safe(module)
            if purpose:
                module.purpose_statement = purpose
                self.purpose_cache[path] = purpose
                processed += 1
            
            # Stop if we detected invalid key
            if self.invalid_key_detected:
                print("⚠️  Stopping analysis due to invalid API key")
                self.disabled = True
                break
        
        # Cluster modules into domains if we have enough
        if self.embedder and len(self.purpose_cache) >= 5 and not self.disabled:
            self._cluster_into_domains(modules)
        
        # Detect documentation drift
        if not self.disabled:
            self._detect_documentation_drift(modules)
        
        print(f"✅ Semanticist: Generated {processed} purpose statements")
        print(f"  API calls: {self.api_calls}, Errors: {self.api_errors}")
        
        if self.api_errors > self.api_calls / 2:
            print("⚠️  High error rate detected. Check your API key and network connection.")
        
        return modules
    
    def _generate_purpose_safe(self, module: ModuleNode) -> Optional[str]:
        """Generate purpose statement with safe error handling."""
        if self.disabled or self.invalid_key_detected:
            return None
        
        try:
            return self._generate_purpose_with_retry(module)
        except Exception as e:
            self.api_errors += 1
            
            # Check for authentication errors
            if self._is_auth_error(e):
                print(f"❌ AUTHENTICATION ERROR: Invalid API key")
                print("   Please check your GROQ_API_KEY in .env file")
                print("   Get a new key at: https://console.groq.com/keys")
                self.invalid_key_detected = True
                self.disabled = True
            else:
                # Only print detailed error for non-auth errors
                error_msg = str(e)[:200]  # Limit error message length
                print(f"⚠️  Error for {module.path}: {error_msg}")
            
            return None
    
    def _generate_purpose_with_retry(self, module: ModuleNode, max_retries: int = 3) -> Optional[str]:
        """Generate purpose statement with retry logic and rate limiting."""
        if not self.groq_api_key:
            return None
        
        for attempt in range(max_retries):
            try:
                # Apply rate limiting before API call
                self.rate_limiter.wait_if_needed()
                
                # Read module content using safe method
                full_path = self.repo_path / module.path
                content = self._safe_read_file(full_path)
                if content is None:
                    return None
                
                # Truncate if too long
                if len(content) > 3000:
                    content = content[:3000] + "..."
                
                # Estimate tokens and check budget
                tokens = self.context_budget.estimate_tokens(content)
                if not self.context_budget.can_call(tokens, "llama-3.1-8b-instant"):
                    print(f"⚠️  Budget exceeded for {module.path}, skipping")
                    return None
                
                # Import here to avoid dependency issues
                from langchain_groq import ChatGroq
                from langchain_core.messages import HumanMessage, SystemMessage
                
                # Initialize Groq with proper error handling
                try:
                    llm = ChatGroq(
                        temperature=0.1,
                        api_key=self.groq_api_key,  # Can be string directly
                        model="llama-3.1-8b-instant",
                        max_retries=2,
                        timeout=30
                    )
                except Exception as e:
                    if "api_key" in str(e).lower():
                        print(f"❌ Error initializing Groq: Invalid API key format")
                        self.invalid_key_detected = True
                    raise
                
                # Create prompt
                system_message = SystemMessage(
                    content="You are an expert code analyst. Generate a 2-3 sentence purpose statement explaining what this code does. Focus on business function, not implementation details. Be concise and specific."
                )
                
                human_message = HumanMessage(
                    content=f"File: {module.path}\n\nCode:\n```python\n{content}\n```\n\nPurpose statement:"
                )
                
                # Make API call
                response = llm.invoke([system_message, human_message])
                self.api_calls += 1
                
                # Track usage
                self.context_budget.track_usage(
                    str(human_message.content),
                    response.content,
                    "llama-3.1-8b-instant"
                )
                
                return response.content.strip()
                
            except Exception as e:
                self.api_errors += 1
                error_str = str(e).lower()
                
                # Check for authentication errors
                if self._is_auth_error(e):
                    print(f"❌ Authentication failed: Invalid API key")
                    self.invalid_key_detected = True
                    raise  # Re-raise to be caught by _generate_purpose_safe
                
                # Check for rate limit errors
                elif self._is_rate_limit_error(e):
                    wait_time = 2 ** attempt  # Exponential backoff
                    print(f"⏱️  Rate limit hit, waiting {wait_time}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(wait_time)
                
                # Check for quota exceeded
                elif "quota" in error_str or "exceeded" in error_str:
                    print(f"⚠️  API quota exceeded. Try again later or upgrade your plan.")
                    time.sleep(10)
                    if attempt >= max_retries - 1:
                        return None
                
                # Check for field limit error
                elif "field larger than field limit" in error_str:
                    print(f"⚠️  CSV field limit error for {module.path}")
                    return None
                
                # Check for overflow error
                elif "overflow" in error_str or "int too large" in error_str:
                    print(f"⚠️  Integer overflow error for {module.path}")
                    return None
                
                # Other errors
                elif attempt < max_retries - 1:
                    # Transient error, retry
                    wait_time = 2 ** attempt
                    print(f"⏱️  Transient error, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    # Last attempt failed
                    print(f"⚠️  Failed after {max_retries} attempts: {str(e)[:100]}")
        
        return None
    
    def _detect_documentation_drift(self, modules: Dict[str, ModuleNode]):
        """Detect discrepancies between docstrings and implementation."""
        for path, module in modules.items():
            if module.language != 'python' or not module.purpose_statement:
                continue
            
            try:
                # Read file using safe method
                full_path = self.repo_path / path
                content = self._safe_read_file(full_path)
                if content is None:
                    continue
                
                # Extract docstring (simple heuristic)
                docstring_match = re.search(r'"""(.*?)"""', content, re.DOTALL)
                
                if docstring_match:
                    docstring = docstring_match.group(1).strip()
                    
                    # Simple drift detection based on length difference
                    doc_len = len(docstring)
                    purpose_len = len(module.purpose_statement)
                    
                    if abs(doc_len - purpose_len) > 100:
                        self.doc_drift_flags.append({
                            "module": path,
                            "docstring": docstring[:100] + "..." if len(docstring) > 100 else docstring,
                            "generated": module.purpose_statement,
                            "drift_score": abs(doc_len - purpose_len) / max(doc_len, 1)
                        })
            
            except Exception as e:
                # Silently ignore docstring extraction errors
                pass
    
    def _cluster_into_domains(self, modules: Dict[str, ModuleNode]):
        """Cluster modules into business domains based on purpose statements."""
        if not self.embedder:
            return
        
        try:
            from sklearn.cluster import KMeans
            
            # Get modules with purpose statements
            valid_modules = [
                (path, module) for path, module in modules.items()
                if module.purpose_statement
            ]
            
            if len(valid_modules) < 5:
                return
            
            paths, module_list = zip(*valid_modules)
            purposes = [m.purpose_statement for m in module_list]
            
            # Generate embeddings
            embeddings = self.embedder.encode(purposes)
            
            # Cluster
            n_clusters = min(5, len(valid_modules) // 3)
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            labels = kmeans.fit_predict(embeddings)
            
            # Label clusters
            cluster_labels = [
                "ingestion", "transformation", "analytics", 
                "monitoring", "core_business", "infrastructure"
            ]
            
            # Assign domains
            for path, label in zip(paths, labels):
                if path in modules:
                    modules[path].domain_cluster = cluster_labels[label % len(cluster_labels)]
                    
        except Exception as e:
            print(f"⚠️  Clustering failed: {e}")
    
    def answer_day_one_questions(self, modules: Dict[str, ModuleNode], 
                                 lineage_graph) -> Dict[str, str]:
        """Answer the five FDE Day-One questions with rate limiting."""
        
        if not self.api_key_valid or self.disabled:
            return {
                "1": "**Primary Data Ingestion Path**\n\nAPI key not configured or invalid. Set a valid GROQ_API_KEY in .env file to enable this feature.",
                "2": "**Critical Output Datasets**\n\nAPI key not configured or invalid. Set a valid GROQ_API_KEY in .env file to enable this feature.",
                "3": "**Blast Radius Analysis**\n\nAPI key not configured or invalid. Set a valid GROQ_API_KEY in .env file to enable this feature.",
                "4": "**Business Logic Distribution**\n\nAPI key not configured or invalid. Set a valid GROQ_API_KEY in .env file to enable this feature.",
                "5": "**Change Velocity**\n\nAPI key not configured or invalid. Set a valid GROQ_API_KEY in .env file to enable this feature."
            }
        
        # Apply rate limiting
        self.rate_limiter.wait_if_needed()
        
        # Prepare context
        context = self._prepare_day_one_context(modules, lineage_graph)
        
        try:
            # Use expensive model for synthesis
            from langchain_groq import ChatGroq
            from langchain_core.messages import HumanMessage, SystemMessage
            
            llm = ChatGroq(
                temperature=0.1,
                api_key=self.groq_api_key,
                model="llama-3.1-8b-instant",
                max_retries=2,
                timeout=60
            )
            
            system_message = SystemMessage(
                content="You are an expert Forward Deployed Engineer. Based on the "
                       "codebase analysis provided, answer the five critical "
                       "Day-One questions. Provide specific file paths and line "
                       "numbers as evidence where possible. Be concise and accurate."
            )
            
            questions = """
            1. What is the primary data ingestion path?
            2. What are the 3-5 most critical output datasets/endpoints?
            3. What is the blast radius if the most critical module fails?
            4. Where is the business logic concentrated vs. distributed?
            5. What has changed most frequently in the last 90 days?
            """
            
            human_message = HumanMessage(
                content=f"Codebase Analysis:\n{context}\n\n"
                       f"Questions:\n{questions}\n\n"
                       f"Please answer each question with evidence:"
            )
            
            response = llm.invoke([system_message, human_message])
            self.api_calls += 1
            
            # Parse into structured answers
            answers = {}
            current_q = None
            current_answer = []
            
            for line in response.content.split('\n'):
                if line.strip() and line[0].isdigit() and '.' in line:
                    if current_q:
                        answers[current_q] = '\n'.join(current_answer)
                    current_q = line.split('.')[0].strip()
                    current_answer = [line]
                elif current_q:
                    current_answer.append(line)
            
            if current_q:
                answers[current_q] = '\n'.join(current_answer)
            
            return answers
            
        except Exception as e:
            print(f"⚠️  Error answering day-one questions: {e}")
            self.api_errors += 1
            
            # Check for auth error
            if self._is_auth_error(e):
                self.invalid_key_detected = True
                self.disabled = True
                return {
                    "1": "**Authentication Error**\n\nInvalid API key. Please check your GROQ_API_KEY in .env file.",
                    "2": "**Authentication Error**\n\nInvalid API key. Please check your GROQ_API_KEY in .env file.",
                    "3": "**Authentication Error**\n\nInvalid API key. Please check your GROQ_API_KEY in .env file.",
                    "4": "**Authentication Error**\n\nInvalid API key. Please check your GROQ_API_KEY in .env file.",
                    "5": "**Authentication Error**\n\nInvalid API key. Please check your GROQ_API_KEY in .env file."
                }
            
            return {
                "1": f"Error generating answer: {str(e)[:200]}",
                "2": f"Error generating answer: {str(e)[:200]}",
                "3": f"Error generating answer: {str(e)[:200]}",
                "4": f"Error generating answer: {str(e)[:200]}",
                "5": f"Error generating answer: {str(e)[:200]}"
            }
    
    def _prepare_day_one_context(self, modules: Dict[str, ModuleNode], 
                                 lineage_graph) -> str:
        """Prepare context for Day-One questions."""
        context_parts = []
        
        # Module summary
        context_parts.append("=== MODULE SUMMARY ===")
        for path, module in list(modules.items())[:20]:  # Limit to 20 modules
            context_parts.append(
                f"- {path}: {module.purpose_statement or 'Unknown'} "
                f"(domain: {module.domain_cluster or 'unknown'})"
            )
        
        # Data lineage
        context_parts.append("\n=== DATA LINEAGE ===")
        if hasattr(lineage_graph, 'nodes'):
            datasets = [n for n in lineage_graph.nodes() if 'dataset:' in n]
            context_parts.append(f"Found {len(datasets)} datasets")
            
            # Find sources and sinks
            sources = []
            sinks = []
            for node in lineage_graph.nodes():
                if 'dataset:' in node:
                    if lineage_graph.in_degree(node) == 0:
                        sources.append(node.replace('dataset:', ''))
                    if lineage_graph.out_degree(node) == 0:
                        sinks.append(node.replace('dataset:', ''))
            
            context_parts.append(f"Source datasets: {', '.join(sources[:5])}")
            context_parts.append(f"Sink datasets: {', '.join(sinks[:5])}")
        
        # Git velocity
        context_parts.append("\n=== CHANGE VELOCITY ===")
        high_velocity = sorted(
            [(p, m.change_velocity_30d) for p, m in modules.items()],
            key=lambda x: x[1], reverse=True
        )[:10]
        context_parts.append("Top changing files:")
        for path, velocity in high_velocity:
            if velocity > 0:
                context_parts.append(f"- {path}: {velocity} changes")
        
        return '\n'.join(context_parts)