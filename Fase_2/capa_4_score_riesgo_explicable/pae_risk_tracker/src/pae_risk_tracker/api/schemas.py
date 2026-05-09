from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SearchContractsRequest(BaseModel):
    query: Optional[str] = None
    entity_name: Optional[str] = None
    department: Optional[str] = None
    municipality: Optional[str] = None
    supplier_name: Optional[str] = None
    modality: Optional[str] = None
    state: Optional[str] = None
    min_amount: Optional[float] = None
    max_amount: Optional[float] = None
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


class AgentQueryRequest(BaseModel):
    query: str
    limit: int = Field(default=10, ge=1, le=50)


class ApiResponse(BaseModel):
    status: str


class SearchContractsResponse(BaseModel):
    source_table: str
    total_rows: int
    returned_rows: int
    rows: List[Dict[str, Any]]


class SearchRecordsResponse(BaseModel):
    source_table: str
    total_rows: int
    returned_rows: int
    rows: List[Dict[str, Any]]


class ContractRiskResponse(BaseModel):
    source_table: str
    contract_id: str
    found: bool
    row: Optional[Dict[str, Any]] = None
    risk: Optional[Dict[str, Any]] = None


class HighRiskResponse(BaseModel):
    source_table: str
    threshold: int
    total_rows: int
    returned_rows: int
    rows: List[Dict[str, Any]]


class AgentQueryResponse(BaseModel):
    query: str
    plan: Dict[str, Any]
    source_table: str
    total_rows: int
    returned_rows: int
    rows: List[Dict[str, Any]]
    evidence_rows: List[Dict[str, Any]]
    validation: Dict[str, Any]
    analysis: Dict[str, Any]
    llm_mode: str
    llm_model: str


class ValidationLatestResponse(BaseModel):
    latest_run: Dict[str, Any]
    observations: List[Dict[str, Any]]
    observation_count: int


class ValidationContractResponse(BaseModel):
    contract_id: str
    observation_count: int
    observations: List[Dict[str, Any]]


class DiagnosticCaseResponse(BaseModel):
    case_id: str
    source_kind: str
    stage: str
    contract_id: str
    process_id: str
    entity: str
    supplier: str
    department: str
    municipality: str
    modality: str
    risk_score: int
    risk_level: str
    primary_flags: List[str]
    diagnosis: str
    review_checklist: List[str]


class ProcessDiagnosticsResponse(BaseModel):
    generated_at: str
    overall_status: str
    source_table: str
    total_records: int
    real_case_count: int
    synthetic_case_count: int
    high_risk_count: int
    medium_risk_count: int
    low_risk_count: int
    traceability_gap_count: int
    criteria_status: str
    criteria_coverage_ratio: float
    validation_status: str
    criteria: Dict[str, Any]
    validation: Dict[str, Any]
    top_departments: List[Dict[str, Any]]
    top_suppliers: List[Dict[str, Any]]
    top_modalities: List[Dict[str, Any]]
    process_steps: List[Dict[str, Any]]
    real_cases: List[DiagnosticCaseResponse]
    synthetic_cases: List[DiagnosticCaseResponse]
    gaps: List[str]


class ChatRequest(BaseModel):
    query: str = Field(default="", max_length=5000)
    session_id: str = Field(default="default", max_length=128)
    limit: int = Field(default=10, ge=1, le=20)


class ChatResponse(BaseModel):
    session_id: str
    intent: str
    message: str
    view_type: str
    data: Dict[str, Any] = Field(default_factory=dict)
    suggested_actions: List[str] = Field(default_factory=list)
    limitations: str
    session_state: Dict[str, Any] = Field(default_factory=dict)
    meta: Dict[str, Any] = Field(default_factory=dict)
