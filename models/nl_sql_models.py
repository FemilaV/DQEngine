"""
Data models for the NL→SQL Agent.
Typed inputs and outputs — no raw dicts between nodes.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class NLSQLRequest:
    """What the user asks for."""
    natural_language: str           # "show me orders with missing customer ids"
    source_config: Dict[str, Any]   # Which database to run against
    target_tables: List[str] = field(default_factory=list)  # Optional hints
    max_rows: int = 100             # Safety limit


@dataclass
class GeneratedSQL:
    """The SQL the LLM produced, before execution."""
    sql: str
    explanation: str                # Plain English: what this SQL does
    tables_used: List[str]
    confidence: float               # 0.0–1.0 — how sure the LLM is
    warnings: List[str] = field(default_factory=list)   # e.g. "no date column found"


@dataclass  
class NLSQLResult:
    """Final result returned to the caller."""
    success: bool
    natural_language: str           # Original question
    generated_sql: Optional[str]
    explanation: Optional[str]
    
    # Query execution results
    data: Any = None                # pandas DataFrame
    row_count: int = 0
    columns: List[str] = field(default_factory=list)
    execution_time_ms: float = 0.0
    
    # Error info
    error: Optional[str] = None
    
    # LLM metadata
    llm_provider: str = ""
    model_used: str = ""
    tokens_used: int = 0

    def to_dict(self) -> Dict:
        return {
            "success": self.success,
            "natural_language": self.natural_language,
            "generated_sql": self.generated_sql,
            "explanation": self.explanation,
            "row_count": self.row_count,
            "columns": self.columns,
            "execution_time_ms": self.execution_time_ms,
            "error": self.error,
            "llm_provider": self.llm_provider,
            "model_used": self.model_used,
            "data": self.data.to_dict(orient="records") if self.data is not None else [],
        }


@dataclass
class NLSQLAgentState:
    """LangGraph state — flows through every node."""
    # Input
    request: Optional[NLSQLRequest] = None

    # Built during execution
    schema_context: str = ""        # Formatted schema fed to LLM
    available_tables: List[str] = field(default_factory=list)
    generated: Optional[GeneratedSQL] = None
    
    # Output
    result: Optional[NLSQLResult] = None

    # Control
    error: Optional[str] = None
    current_step: str = "init"
    retry_count: int = 0
    logs: List[str] = field(default_factory=list)

    def log(self, msg: str):
        import logging
        self.logs.append(msg)
        logging.getLogger("nl_sql_agent").info(msg)