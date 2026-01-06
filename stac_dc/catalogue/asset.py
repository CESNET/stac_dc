from dataclasses import dataclass

@dataclass
class Asset:
    key: str
    href: str
    type: str
    title: str
