"""Context window management for LLM calls."""

import tiktoken
from typing import List, Dict, Any
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class TokenUsage:
    """Track token usage for LLM calls."""
    
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cost: float = 0.0
    model: str = ""
    timestamp: datetime = None


class ContextWindowBudget:
    """Manage token budgets and costs for LLM calls."""
    
    # Cost per 1K tokens (approximate)
    MODEL_COSTS = {
        "mixtral-8x7b-32768": {"input": 0.0003, "output": 0.0003},
        "llama2-70b-4096": {"input": 0.0007, "output": 0.0007},
        "gemini-flash": {"input": 0.0001, "output": 0.0002},
        "gpt-4": {"input": 0.03, "output": 0.06},
    }
    
    def __init__(self, max_budget: float = 1.0):
        """Initialize with a maximum budget in USD."""
        self.max_budget = max_budget
        self.total_cost = 0.0
        self.usage_history: List[TokenUsage] = []
        self.tokenizer = tiktoken.get_encoding("cl100k_base")
    
    def estimate_tokens(self, text: str) -> int:
        """Estimate the number of tokens in a text."""
        return len(self.tokenizer.encode(text))
    
    def can_call(self, estimated_tokens: int, model: str) -> bool:
        """Check if we can make a call within budget."""
        cost_per_1k = self.MODEL_COSTS.get(model, self.MODEL_COSTS["mixtral-8x7b-32768"])
        estimated_cost = (estimated_tokens / 1000) * cost_per_1k["input"]
        return self.total_cost + estimated_cost <= self.max_budget
    
    def track_usage(self, prompt: str, completion: str, model: str):
        """Track token usage and cost after a call."""
        prompt_tokens = self.estimate_tokens(prompt)
        completion_tokens = self.estimate_tokens(completion)
        total_tokens = prompt_tokens + completion_tokens
        
        cost_per_1k = self.MODEL_COSTS.get(model, self.MODEL_COSTS["mixtral-8x7b-32768"])
        cost = (prompt_tokens / 1000) * cost_per_1k["input"] + \
               (completion_tokens / 1000) * cost_per_1k["output"]
        
        usage = TokenUsage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            cost=cost,
            model=model,
            timestamp=datetime.now()
        )
        
        self.usage_history.append(usage)
        self.total_cost += cost
    
    def get_tiered_model(self, tokens: int) -> str:
        """Select appropriate model based on token count."""
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
            "remaining_budget": self.max_budget - self.total_cost
        }