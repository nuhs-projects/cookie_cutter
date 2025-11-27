from confluent_kafka import Producer, KafkaException 
import utils.gen_utils as gu
import utils.log_utils as lu
import logging
import socket
import json
from datetime import datetime 
import time
from kafka.producer import KProducer

args = gu.setup_args()
logger = logging.getLogger(__name__)
lu.setup_logger(logLevel = args['LOG_LEVEL'])

config = {
        "bootstrap.servers": args["bootstrap.servers"],
        "sasl.mechanism": args["sasl.mechanism"],
        "security.protocol": args["security.protocol"],
        "ssl.ca.location": args["ssl.ca.location"],
        "sasl.username": args["sasl.username_producer"],
        "sasl.password": args["sasl.password_producer"],
        "client.id": f"{args['PROJECT_NAME']}_{socket.gethostname()}",
        "retry.backoff.ms": args["retry.backoff.ms"],
        "enable.ssl.certificate.verification": False
    }

def delivery_report(err, msg): 
    if err is not None: 
        print("message delivery failed: {}".format(err))
    else: 
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

if __name__ == "__main__": 
    p = Producer(config)
    data = """MSH|^~\&|EPIC|N1SG|||20250915121803|SYNSONIAT|ADT^A02|12160271|T|2.3||||||UNICODE UTF-8\rEVN|A08|20250915121803||VISIT_LEVEL_CHANGE|SYNSONIAT^TAN^PENG^HWEE SONIA^^^^^JHS^^^^^N1SG\rPID|1|R0008055A^^^NRIC^NRIC|R0008055A^^^P||DEE LEE^^^^MS.||19930208|F||CN|10^^^JALAN SOO BEE^488113^SG^P~~||9626 2352^P^MOBILE||EL|||1452854|R0008055A|||||||||SG||A\rZPD|||||||N|||||||||||N||||||F||||||||N|N\rPV1|1|IP|NCZSCTC^NUH STEM CELL THERAPY CENTRE POOL ROOM^NONE^N1SG^R||||08710F^PENG^YONG^WEI^^^^^MCR^^^^MCR|||NSMEGEN|||||||P2914C^YING^CLAIRE^TAY ERN^^^^^MCR^^^^MCR||100700036811|SELF||||||||||||||||01|NA|||| |^^^NTFH||20250304155800|20250915121200\rPV2||BBSA||||||20250304155200||||3||||||||||N||||||||||N||||||||A\rZPV|||||||||||20250304155800||||||||||None|None\rOBX|1|ID|OP_Case_End_Type|1|01||||||F|||20250304155800\rOBX|2|DTM|OP_Case_End_Date|2|20250915121200||||||F|||20250304155800\rOBX|3|TX|CITY^CITY|1|Singapore\rOBX|4|TX|Stu_Indicator^STUDENT VISA?|1|N\rOBX|5|TX|Donor_Indicator^ADT - ORGAN DONOR|1|N\rOBX|6|TX|Admission_Reason_One^ADMITTING CATEGORY|1|1|||||||||20250304\rOBX|7|NM|IP_Referral_Type^ADMISSION SOURCE LOCATION|1|07^Intra-Dept referral A\T\E CUCC {INA\T\E}~ZZZ0704^Intra-Dept referral A\T\E CUCC {INA\T\E}|||||||||20250304\rOBX|8|TX|FP_Program^REG ADDL PAT CATEGORY 2|1|N\rOBX|9|TX|Discharge_Physician^HOSPITAL - DISCHARGE PROVIDER|1|08710F^PENG^YONG^WEI^^^^^MCR^^^^MCR|||||||||20250304\rOBX|10|TX|Discharge_Disposition^DISCHARGE DISPOSITION|1|01|||||||||20250304\rOBX|11|TX|IP_Admission_Type^HOSPITAL ADMISSION STATUS|1|EM|||||||||20250304\rOBX|12|TX|Sub_Doc_Number^SINGAPORE - SUBVENTION DOCUMENT ID|1|S2300034B|||||||||20250304\rOBX|13|TX|Sub_Doc_Type^SINGAPORE - SUBVENTION DOCUMENT TYPE|1|P|||||||||20250304\rOBX|14|DT|Sub_Doc_Expiry_Date^SINGAPORE - SUBVENTION DOCUMENT EXPIRATION DATE|1|21001231|||||||||20250304MSH|^~\&|EPIC|N1SG|||20250915121803|SYNSONIAT|ADT^A08|12160271|T|2.3||||||UNICODE UTF-8\rEVN|A08|20250915121803||VISIT_LEVEL_CHANGE|SYNSONIAT^TAN^PENG^HWEE SONIA^^^^^JHS^^^^^N1SG\rPID|1|S9000095J^^^NRIC^NRIC|S9000095J^^^P||DEE LEE^^^^MS.||19930208|F||CN|10^^^JALAN SOO BEE^488113^SG^P~~||9626 2352^P^MOBILE||EL|||1452854|S9000095J|||||||||SG||A\rZPD|||||||N|||||||||||N||||||F||||||||N|N\rPV1|1|IP|NCZSCTC^NUH STEM CELL THERAPY CENTRE POOL ROOM^NONE^N1SG^Ri||||08710F^PENG^YONG^WEI^^^^^MCR^^^^MCR|||NSMEGEN|||||||P2914C^YING^CLAIRE^TAY ERN^^^^^MCR^^^^MCR||100700036811|SELF||||||||||||||||01|NA|||| |^^^NTFH||20250304155800|20250915121200\rPV2||BBSA||||||20250304155200||||3||||||||||N||||||||||N||||||||A\rZPV|||||||||||20250304155800||||||||||None|None\rOBX|1|ID|OP_Case_End_Type|1|01||||||F|||20250304155800\rOBX|2|DTM|OP_Case_End_Date|2|20250915121200||||||F|||20250304155800\rOBX|3|TX|CITY^CITY|1|Singapore\rOBX|4|TX|Stu_Indicator^STUDENT VISA?|1|N\rOBX|5|TX|Donor_Indicator^ADT - ORGAN DONOR|1|N\rOBX|6|TX|Admission_Reason_One^ADMITTING CATEGORY|1|1|||||||||20250304\rOBX|7|NM|IP_Referral_Type^ADMISSION SOURCE LOCATION|1|07^Intra-Dept referral A\T\E CUCC {INA\T\E}~ZZZ0704^Intra-Dept referral A\T\E CUCC {INA\T\E}|||||||||20250304\rOBX|8|TX|FP_Program^REG ADDL PAT CATEGORY 2|1|N\rOBX|9|TX|Discharge_Physician^HOSPITAL - DISCHARGE PROVIDER|1|08710F^PENG^YONG^WEI^^^^^MCR^^^^MCR|||||||||20250304\rOBX|10|TX|Discharge_Disposition^DISCHARGE DISPOSITION|1|01|||||||||20250304\rOBX|11|TX|IP_Admission_Type^HOSPITAL ADMISSION STATUS|1|EM|||||||||20250304\rOBX|12|TX|Sub_Doc_Number^SINGAPORE - SUBVENTION DOCUMENT ID|1|S2300034B|||||||||20250304\rOBX|13|TX|Sub_Doc_Type^SINGAPORE - SUBVENTION DOCUMENT TYPE|1|P|||||||||20250304\rOBX|14|DT|Sub_Doc_Expiry_Date^SINGAPORE - SUBVENTION DOCUMENT EXPIRATION DATE|1|21001231|||||||||20250304"""
    start = datetime.now()
    for i in range(1000):
        mrn = i
        producer = KProducer(args,topics="nuhs.eai.antenatal")
        if i < 4: 
            logger.info(data)
        producer.produce(data)

        if i % 15 == 0:
            now = datetime.now()
            duration = (now - start).total_seconds()
            if duration < 1:
                time.sleep(1-duration)
            start = datetime.now()
    producer.flush()