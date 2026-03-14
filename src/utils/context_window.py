"""Context window budget with token counting for API cost management."""

import tiktoken
from typing import List, Dict, Any, Optional
from datetime import datetime
import json


class TokenUsage:
    """Track token usage for LLM calls."""
    
    def __init__(self, prompt_tokens: int = 0, completion_tokens: int = 0, 
                 model: str = "", cost: float = 0.0):
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.total_tokens = prompt_tokens + completion_tokens
        self.model = model
        self.cost = cost
        self.timestamp = datetime.now()


class ContextWindowBudget:
    """Manage token budgets and costs for LLM calls."""
    
    # Model context limits
    MODEL_LIMITS = {
        "mixtral-8x7b-32768": 32768,
        "llama2-70b-4096": 4096,
        "gemini-flash": 16384,
        "gpt-4": 8192,
    }
    
    # Cost per 1K tokens (approximate in USD)
    MODEL_COSTS = {
        "mixtral-8x7b-32768": {"input": 0.0003, "output": 0.0003},
        "llama2-70b-4096": {"input": 0.0007, "output": 0.0007},
        "gemini-flash": {"input": 0.0001, "output": 0.0002},
        "gpt-4": {"input": 0.03, "output": 0.06},
    }
    
    def __init__(self, max_budget: float = 1.0):
        """
        Initialize with a maximum budget in USD.
        
        Args:
            max_budget: Maximum total cost allowed in USD
        """
        self.max_budget = max_budget
        self.total_cost = 0.0
        self.usage_history: List[TokenUsage] = []
        
        # Initialize tokenizer
        try:
            self.tokenizer = tiktoken.get_encoding("cl100k_base")
        except:
            self.tokenizer = None
            print("⚠️  tiktoken not available, using approximate token counting")
    
    def estimate_tokens(self, text: str) -> int:
        """
        Estimate the number of tokens in a text.
        
        Args:
            text: Input text
            
        Returns:
            Estimated token count
        """
        if not text:
            return 0
        
        if self.tokenizer:
            try:
                return len(self.tokenizer.encode(text))
            except:
                pass
        
        # Rough approximation: 4 characters per token
        return len(text) // 4
    
    def can_call(self, estimated_tokens: int, model: str) -> bool:
        """
        Check if we can make a call within budget.
        
        Args:
            estimated_tokens: Estimated prompt tokens
            model: Model name
            
        Returns:
            True if call is within budget
        """
        # Check model limit
        model_limit = self.MODEL_LIMITS.get(model, 4096)
        if estimated_tokens > model_limit:
            return False
        
        # Check budget
        cost_per_1k = self.MODEL_COSTS.get(model, self.MODEL_COSTS["mixtral-8x7b-32768"])
        estimated_cost = (estimated_tokens / 1000) * cost_per_1k["input"]
        
        return self.total_cost + estimated_cost <= self.max_budget
    
    def track_usage(self, prompt: str, completion: str, model: str):
        """
        Track token usage and cost after a call.
        
        Args:
            prompt: Prompt text
            completion: Completion text
            model: Model name
        """
        prompt_tokens = self.estimate_tokens(prompt)
        completion_tokens = self.estimate_tokens(completion)
        
        cost_per_1k = self.MODEL_COSTS.get(model, self.MODEL_COSTS["mixtral-8x7b-32768"])
        cost = (prompt_tokens / 1000) * cost_per_1k["input"] + \
               (completion_tokens / 1000) * cost_per_1k["output"]
        
        usage = TokenUsage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            model=model,
            cost=cost
        )
        
        self.usage_history.append(usage)
        self.total_cost += cost
    
    def get_tiered_model(self, tokens: int) -> str:
        """
        Select appropriate model based on token count.
        
        Args:
            tokens: Estimated token count
            
        Returns:
            Model name
        """
        if tokens < 2000:
            return "mixtral-8x7b-32768"  # Fast model
        elif tokens < 10000:
            return "llama2-70b-4096"  # Medium model
        else:
            return "gpt-4"  # Expensive model for complex tasks
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of usage."""
        return {
            "total_cost": self.total_cost,
            "total_calls": len(self.usage_history),
            "total_tokens": sum(u.total_tokens for u in self.usage_history),
            "max_budget": self.max_budget,
            "remaining_budget": self.max_budget - self.total_cost,
            "average_cost_per_call": self.total_cost / max(len(self.usage_history), 1)
        }
    
    def reset(self):
        """Reset budget tracking."""
        self.total_cost = 0.0
        self.usage_history = []