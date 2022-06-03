from datetime import datetime
from ssi.helpers.datetime_helper import calc_period_range
from ssi.models.constants import PERIOD_TYPE

class TestFunctionCalcPeriodRange:
    def test_mtd(self):
        business_date: datetime = datetime.strptime("26/05/2022", "%d/%m/%Y")
        period_type: PERIOD_TYPE = PERIOD_TYPE.MTD
        from_date, to_date = calc_period_range(business_date=business_date,period_type=period_type)
        assert from_date == datetime.strptime("01/05/2022", "%d/%m/%Y") and to_date == datetime.strptime("26/05/2022", "%d/%m/%Y")

    def test_qtd(self):
        business_date: datetime = datetime.strptime("26/05/2022", "%d/%m/%Y")
        period_type: PERIOD_TYPE = PERIOD_TYPE.QTD
        from_date, to_date = calc_period_range(business_date=business_date,period_type=period_type)
        assert from_date == datetime.strptime("01/04/2022", "%d/%m/%Y") and to_date == datetime.strptime("26/05/2022", "%d/%m/%Y")

    def test_ytd(self):
        business_date: datetime = datetime.strptime("26/05/2022", "%d/%m/%Y")
        period_type: PERIOD_TYPE = PERIOD_TYPE.YTD
        from_date, to_date = calc_period_range(business_date=business_date,period_type=period_type)
        assert from_date == datetime.strptime("01/01/2022", "%d/%m/%Y") and to_date == datetime.strptime("26/05/2022", "%d/%m/%Y")

    def test_m(self):
        business_date: datetime = datetime.strptime("26/05/2022", "%d/%m/%Y")
        period_type: PERIOD_TYPE = PERIOD_TYPE.M
        from_date, to_date = calc_period_range(business_date=business_date,period_type=period_type)
        assert from_date == datetime.strptime("01/05/2022", "%d/%m/%Y") and to_date == datetime.strptime("31/05/2022", "%d/%m/%Y")
    
    def test_q(self):
        business_date: datetime = datetime.strptime("26/05/2022", "%d/%m/%Y")
        period_type: PERIOD_TYPE = PERIOD_TYPE.Q
        from_date, to_date = calc_period_range(business_date=business_date,period_type=period_type)
        assert from_date == datetime.strptime("01/04/2022", "%d/%m/%Y") and to_date == datetime.strptime("30/06/2022", "%d/%m/%Y")

    def test_y(self):
        business_date: datetime = datetime.strptime("26/05/2022", "%d/%m/%Y")
        period_type: PERIOD_TYPE = PERIOD_TYPE.Y
        from_date, to_date = calc_period_range(business_date=business_date,period_type=period_type)
        assert from_date == datetime.strptime("01/01/2022", "%d/%m/%Y") and to_date == datetime.strptime("31/12/2022", "%d/%m/%Y")