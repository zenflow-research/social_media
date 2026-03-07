from datetime import datetime

from pydantic import BaseModel, Field


class PariveshProposal(BaseModel):
    proposal_no: str
    proposal_id: int | None = None
    project_name: str = ""
    company_name: str = ""
    nse_symbol: str = ""  # Matched Nifty 500 symbol
    state: str = ""
    sector: str = ""
    category: str = ""
    clearance_type: str = ""  # EC, FC, WL, CRZ
    proposal_status: str = ""
    date_of_submission: str = ""
    certificate_url: str = ""
    single_window_number: str = ""
    url: str = ""
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    documents_fetched: bool = False
    analyzed: bool = False


class PariveshDocument(BaseModel):
    proposal_no: str
    proposal_id: int | None = None
    doc_uuid: str = ""
    doc_name: str = ""
    doc_type: str = ""  # CERTIFICATE, EDS_QUERY, fc, etc.
    category: str = ""  # documentDetails, agendaDetails, momDetails, cafDetails
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
