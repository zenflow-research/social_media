from datetime import datetime

from pydantic import BaseModel, Field


class PariveshProposal(BaseModel):
    proposal_no: str
    proposal_id: int | None = None
    project_name: str | None = ""
    company_name: str | None = ""
    nse_symbol: str = ""  # Matched Nifty 500 symbol
    state: str | None = ""
    sector: str | None = ""
    category: str | None = ""
    clearance_type: str | None = ""  # EC, FC, WL, CRZ
    proposal_status: str | None = ""
    date_of_submission: str | None = ""
    certificate_url: str | None = ""
    single_window_number: str | None = ""
    url: str = ""
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    documents_fetched: bool = False
    analyzed: bool = False


class PariveshDocument(BaseModel):
    proposal_no: str
    proposal_id: int | None = None
    doc_uuid: str = ""
    doc_name: str | None = ""
    doc_type: str | None = ""  # CERTIFICATE, EDS_QUERY, fc, etc.
    category: str | None = ""  # documentDetails, agendaDetails, momDetails, cafDetails
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
