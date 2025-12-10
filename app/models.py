from pydantic import BaseModel
from typing import Optional, List, Dict, Any

class VariantAttribute(BaseModel):
    id: str
    templateId: Optional[str]
    name: str
    dataType: str
    stringValue: Optional[str] = None
    numberValue: Optional[float] = None
    booleanValue: Optional[bool] = None
    dateValue: Optional[str] = None

class VariantImage(BaseModel):
    id: str
    url: str

class Variant(BaseModel):
    id: str
    sku: str
    price: Optional[float] = 0
    stock: Optional[int] = 0
    images: Optional[List[VariantImage]] = []
    attributes: Optional[List[VariantAttribute]] = []

class PackageAttribute(BaseModel):
    id: str
    templateId: Optional[str]
    name: str
    dataType: str
    stringValue: Optional[str] = None
    numberValue: Optional[float] = None
    booleanValue: Optional[bool] = None
    dateValue: Optional[str] = None

class PackageImage(BaseModel):
    id: str
    url: str

class Package(BaseModel):
    id: str
    name: str
    price: Optional[float] = 0
    description: Optional[str] = ""
    images: Optional[List[PackageImage]] = []
    attributes: Optional[List[PackageAttribute]] = []

class Product(BaseModel):
    id: str
    name: str
    barcode: Optional[str] = None
    description: Optional[str] = ""
    basePrice: Optional[float] = 0
    categoryName: str
    brand: Optional[str] = None
    tags: Optional[List[str]] = []
    variants: Optional[List[Variant]] = []
    attributes: Optional[List[VariantAttribute]] = []

class Service(BaseModel):
    id: str
    name: str
    description: Optional[str] = ""
    basePrice: Optional[float] = 0
    categoryName: str
    tags: Optional[List[str]] = []
    packages: Optional[List[Package]] = []
    attributes: Optional[List[PackageAttribute]] = []
