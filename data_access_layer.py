from sqlalchemy.orm import Session


class DataAccessLayer:
    def __init__(self, db_session: Session):
        self.db_session = db_session

    async def get_bank_data(self):
        pass

    async def get_contact_data(self):
        pass

    async def get_fssp_data(self):
        pass
