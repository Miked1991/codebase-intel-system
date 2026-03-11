"""Semanticist Agent - LLM-powered semantic analysis."""

import os
from pathlib import Path
from typing import Dict, List, Optional, Any
import json
import numpy as np
from sklearn.cluster import KMeans
from sentence_transformers import SentenceTransformer
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage, SystemMessage

from ..models.nodes import ModuleNode
from ..utils.context_window import ContextWindowBudget


class SemanticistAgent:
    """Agent responsible for LLM-powered semantic understanding."""
    
    def __init__(self, repo_path: str, groq_api_key: Optional[str] = None):
        self.repo_path = Path(repo_path)
        self.groq_api_key = groq_api_key or os.getenv('GROQ_API_KEY')
        
        if not self.groq_api_key:
            raise ValueError("GROQ_API_KEY environment variable not set")
        
        # Initialize context budget
        self.context_budget = ContextWindowBudget(max_budget=1.0)  # $1 budget
        
        # Initialize embedding model for clustering
        self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Cache for purpose statements
        self.purpose_cache: Dict[str, str] = {}
        self.doc_drift_flags: List[Dict[str, Any]] = []
    
    def analyze(self, modules: Dict[str, ModuleNode]) -> Dict[str, ModuleNode]:
        """Run semantic analysis on modules."""
        print("🧠 Semanticist: Analyzing module semantics")
        
        # Generate purpose statements for each module
        for path, module in modules.items():
            if module.language == 'python':  # Focus on Python for now
                purpose = self._generate_purpose_statement(module)
                if purpose:
                    module.purpose_statement = purpose
                    self.purpose_cache[path] = purpose
        
        # Cluster modules into domains
        if len(modules) > 5:
            self._cluster_into_domains(modules)
        
        # Detect documentation drift
        self._detect_documentation_drift(modules)
        
        print(f"✅ Semanticist: Analyzed {len(modules)} modules")
        return modules
    
    def _generate_purpose_statement(self, module: ModuleNode) -> Optional[str]:
        """Generate a purpose statement for a module using LLM."""
        try:
            # Read module content
            full_path = self.repo_path / module.path
            with open(full_path, 'r') as f:
                content = f.read()
            
            # Truncate if too long
            if len(content) > 4000:
                content = content[:4000] + "..."
            
            # Estimate tokens
            tokens = self.context_budget.estimate_tokens(content)
            
            # Select model based on token count
            model = self.context_budget.get_tiered_model(tokens)
            
            # Check budget
            if not self.context_budget.can_call(tokens, model):
                print(f"⚠️  Budget exceeded for {module.path}, skipping")
                return None
            
            # Initialize Groq
            llm = ChatGroq(
                temperature=0.1,
                groq_api_key=self.groq_api_key,
                model_name=model
            )
            
            # Create prompt
            system_message = SystemMessage(
                content="You are an expert code analyst. Your task is to generate "
                       "a concise purpose statement for a code module. Focus on "
                       "WHAT the module does, not HOW. Keep it to 2-3 sentences."
            )
            
            human_message = HumanMessage(
                content=f"Analyze this Python module and tell me its purpose:\n\n"
                       f"File: {module.path}\n\n"
                       f"Code:\n```python\n{content}\n```\n\n"
                       f"Purpose statement (2-3 sentences):"
            )
            
            # Get response
            response = llm.invoke([system_message, human_message])
            
            # Track usage
            self.context_budget.track_usage(
                str(human_message.content),
                response.content,
                model
            )
            
            return response.content.strip()
            
        except Exception as e:
            print(f"⚠️  Error generating purpose for {module.path}: {e}")
            return None
    
    def _detect_documentation_drift(self, modules: Dict[str, ModuleNode]):
        """Detect discrepancies between docstrings and implementation."""
        for path, module in modules.items():
            if module.language != 'python':
                continue
            
            try:
                # Read file
                full_path = self.repo_path / path
                with open(full_path, 'r') as f:
                    content = f.read()
                
                # Extract docstring (simple heuristic)
                import re
                docstring_match = re.search(r'"""(.*?)"""', content, re.DOTALL)
                
                if docstring_match and module.purpose_statement:
                    docstring = docstring_match.group(1).strip()
                    
                    # Check if they're similar (simple length comparison)
                    doc_len = len(docstring)
                    purpose_len = len(module.purpose_statement)
                    
                    # If lengths are very different, flag drift
                    if abs(doc_len - purpose_len) > 100:
                        self.doc_drift_flags.append({
                            "module": path,
                            "docstring": docstring[:100] + "...",
                            "generated": module.purpose_statement,
                            "drift_score": abs(doc_len - purpose_len) / max(doc_len, 1)
                        })
            
            except Exception as e:
                print(f"⚠️  Error detecting drift for {path}: {e}")
    
    def _cluster_into_domains(self, modules: Dict[str, ModuleNode]):
        """Cluster modules into business domains based on purpose statements."""
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
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        labels = kmeans.fit_predict(embeddings)
        
        # Label clusters (simplified - use LLM for better labels in production)
        cluster_labels = [
            "ingestion", "transformation", "serving", 
            "monitoring", "core_business", "infrastructure",
            "testing", "utils"
        ]
        
        # Assign domains
        for path, label in zip(paths, labels):
            if path in modules:
                modules[path].domain_cluster = cluster_labels[label % len(cluster_labels)]
    
    def answer_day_one_questions(self, modules: Dict[str, ModuleNode], 
                                 lineage_graph) -> Dict[str, str]:
        """Answer the five FDE Day-One questions using LLM synthesis."""
        
        # Prepare context
        context = self._prepare_day_one_context(modules, lineage_graph)
        
        try:
            # Use expensive model for synthesis
            llm = ChatGroq(
                temperature=0.2,
                groq_api_key=self.groq_api_key,
                model_name="llama2-70b-4096"  # Use powerful model
            )
            
            system_message = SystemMessage(
                content="You are an expert Forward Deployed Engineer. Based on the "
                       "codebase analysis provided, answer the five critical "
                       "Day-One questions. Provide specific file paths and line "
                       "numbers as evidence."
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
            return {}
    
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
                        sources.append(node)
                    if lineage_graph.out_degree(node) == 0:
                        sinks.append(node)
            
            context_parts.append(f"Source datasets: {sources[:5]}")
            context_parts.append(f"Sink datasets: {sinks[:5]}")
        
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