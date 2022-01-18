from typing import Optional
from datetime import date, datetime
from pydantic import condecimal, validator
from sqlmodel import SQLModel, Field


class Sub(SQLModel, table=True):
    adsh: str = Field(title="Accession Number", primary_key=True)
    cik: str = Field(title="Central Index Key", index = True)
    name: str = Field(title="Name of Registrant") 
    sic: Optional[str] = Field(None, title="Standard Industrial Classification")
    countryba: Optional[str] = Field(None, title="Country Code")
    stprba: Optional[str] = Field(None, title="US or CA State Code")
    cityba: Optional[str] = Field(None, title="City")
    zipba: Optional[str] = Field(None, title="Postal Code")
    bas1: Optional[str] = Field(None, title="Street Address 1")
    bas2: Optional[str] = Field(None, title="Street Address 2")
    baph: Optional[str] = Field(None, title="Telephone")
    countryinc: Optional[str] = Field(None, title="Country of Incorporation")
    stprinc: Optional[str] = Field(None, title="US or CA State of Incorporation Code")
    ein: Optional[str] = Field(None, title="Employee Identification Number")
    former: Optional[str] = Field(None, title="Former Name")
    changed: Optional[str] = Field(None, title="Date of Name Change")
    afs: Optional[str] = Field(None, title="Filer status with the SEC")
    wksi: bool = Field(title="Is Well Known Seasoned Issuer")
    fye: Optional[str] = Field(None, title="Fiscal Year End Date (mmdd)")
    form: str = Field(title="Submission Type")
    period: Optional[date] = Field(None, title="Balance Sheet Date")
    fy: Optional[str] = Field(None, title="Fiscal Year Focus")
    fp: Optional[str] = Field(None, title="Fiscal Period Focus")
    filed: date = Field(title="Filling Date")
    accepted: datetime = Field(title="Acceptance Date and Time")
    prevrpt: bool = Field(title="Is Previous Report")
    detail: bool = Field(title="Has Quantitative Disclosures in Footnotes")
    instance: str = Field(title="Name of Submitted XBRL Doc")
    nciks: int = Field(title="Number of CIKs of registrants")
    aciks:Optional[str] = Field(None, title="Additional CIKs of Co-registrants")

    @validator('*', pre=True)
    def empty_str_to_none(cls, value):
        if len(value) == 0:
            return None
        return value


class Tag(SQLModel, table = True):
    name: str = Field(title="Unique name from a specific taxonomy release", alias = "tag", primary_key=True)
    version: Optional[str] = Field(None, title="Accession number where the tag was defined")
    custom: bool = Field(False, title="Is Custom Tag")
    abstract: bool = Field(False, title="Not numeric Fact")
    datatype: Optional[str] = Field(None, title="Data Type")
    iord: Optional[str] = Field(None, title="I if the value is a point-in time, or D if the value is a duration")
    crdr: Optional[str] = Field(None, title="C for credit, or D for debit")
    tlabel: Optional[str] = Field(None, title="Label Text")
    doc: Optional[str] = Field(None, title="Tag Definition")

    @validator('*', pre=True)
    def empty_str_to_none(cls, value):
        if len(value) == 0:
            return None
        return value
    

class Num(SQLModel, table = True):
    id: Optional[int] = Field(None, primary_key=True)
    sub_adsh: str = Field(title="Accession Number", alias = "adsh", foreign_key="sub.adsh")
    tag_name: str = Field(title="Unique name from a specific taxonomy release", alias = "tag", index=True)
    coreg: Optional[int] = Field(None, title="Co-Registrant")
    ddate: date = Field(title="End Date")
    qrts: Optional[int] = Field(None, title="Number of Quarters Represented")
    uom: str = Field(title="Unit of Measure")
    value: Optional[condecimal(max_digits=28, decimal_places=4)] = Field(None, title = "Value (Not Scaled)")
    footnote: Optional[str] = Field(None, title="Footnote on the Value")

    @validator('*', pre=True)
    def empty_str_to_none(cls, value):
        if len(value) == 0:
            return None
        return value
    

class Pre(SQLModel, table = True):
    id: Optional[int] = Field(None, primary_key=True)
    sub_adsh: str = Field(title="Accession Number", alias = "adsh", foreign_key="sub.adsh")
    report: int = Field(title="Report Grouping")
    line: int = Field(title="Line Order")
    stmt: str = Field(title="Financial Statement Location")
    inpth: bool = Field(title="Is Presented Parenthetically")
    rfile: str = Field(title="Type of Data File")
    tag_name: str = Field(title="Unique name from a specific taxonomy release", alias = "tag", index=True)
    plabel: str = Field(title="Preferred Lable")

    @validator('*', pre=True)
    def empty_str_to_none(cls, value):
        if len(value) == 0:
            return None
        return value
