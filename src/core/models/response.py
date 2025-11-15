from typing import Optional

class Response:
    def __init__(self, allowed: bool, transaction_id: Optional[int] = None):
        self.allowed = allowed
        self.transaction_id = transaction_id

    def __repr__(self):
        return f"Response(allowed={self.allowed}, transaction_id={self.transaction_id})"
