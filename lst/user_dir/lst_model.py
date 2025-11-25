import json
import logging
import pandas as pd
from user_dir.agimodel import AGILos
import nuhs_lst.utils.exception_utils as exception_utils
import nuhs_lst.utils.gen_utils as gen_utils
import nuhs_lst.utils.date_utils as date_utils
import nuhs_lst.utils.log_utils as log_utils

logger = logging.getLogger(__name__)


class LSTModel:
    """Handles the processing and prediction of patient data for ICU/HD departments.

    This model processes incoming messages, enriches data with patient demographics,
    and makes predictions based on the model's learned weights.
    """

    def __init__(self, project_name: str):
        """Initializes the model and loads necessary data.

        Args:
            project_name (str): The name of the project associated with this model.
        """
        self.project_name = project_name
        self.model = AGILos()

        # Load ICU/HD department data
        self.icu_dept_list = self._load_icu_dept_list()

        logger.info(f"ICU/HD departments/wards:\n {self.icu_dept_list}")

    def _load_icu_dept_list(self):
        """Loads and returns ICU/HD deparment list"""

        bed_df = pd.read_csv("./user_dir/reference/bed_class_type_mapping.csv")
        bed_df = bed_df[bed_df["Bed Type"] == "ICU/HD"]
        return list(bed_df["Department"].unique())

    def process_msg(self, msg: str):
        """Processes an incoming message and extracts necessary details.

        Args:
            msg (str): The raw message to process.

        Returns:
            dict: The processed data with relevant information or None if invalid.
        """
        try:
            msg_data = json.loads(msg)
            logger.debug(json.dumps(msg_data, indent=4))

            cmsg = msg_data["L-MSH"][0]
            patient_info = self._extract_patient_info(cmsg)

            # Validate the message based on event type and document type
            if self._is_valid_message(patient_info):
                logger.info(f'Passed processing - uid: {patient_info["uid"]} :)')
                return patient_info
            else:
                logger.info(f'MESSAGE IGNORED -> {patient_info["uid"]}')
        except Exception as error:
            logger.debug(msg)
            logger.error(exception_utils.get_exception_string(error))
            logger.error(f"Unable to parse message. {log_utils.get_crying_sign()}")

        return None

    def _extract_patient_info(self, cmsg: dict) -> dict:
        """Extracts relevant patient information from the message.

        Args:
            cmsg (dict): The parsed message.

        Returns:
            dict: A dictionary containing the extracted patient information.
        """
        patient_info = {
            "mrn": cmsg["S-PatientIdSegment_3"]["C-PatientId_3_02"][
                "E-IdNumber_3_02.01"
            ],
            "event_type_code": cmsg["S-EventTypeUsage_2"]["E-EventTypeCode_2_01"],
            "content": cmsg["S-ObservationResultSegment_6"]["E-ObservationValue_6_05"],
            "document_type": cmsg["S-TranscriptionReportHeaderSegment_5"][
                "E-ReportType_5_02"
            ],
            "msg_dt": cmsg["S-MessageHeaderSegment_1"]["E-DateTimeOfMessage_1_07"],
            "vn": cmsg["S-PatientVisitSegment_4"]["C-VisitNumber_4_19"][
                "E-IdNumber_4_19.01"
            ],
            "dept": cmsg["S-PatientVisitSegment_4"]["C-AssignedPatientLocation_4_03"][
                "E-PointOfCare_4_03.01"
            ],
            "uid": f'{cmsg["S-MessageHeaderSegment_1"]["E-SendingFacility_1_04"]}-'
            f'{cmsg["S-MessageHeaderSegment_1"]["E-DateTimeOfMessage_1_07"]}-'
            f'{cmsg["S-MessageHeaderSegment_1"]["E-MessageControlId_1_10"]}-'
            f'vn-{cmsg["S-PatientVisitSegment_4"]["C-VisitNumber_4_19"]["E-IdNumber_4_19.01"]}-'
            f'dept-{cmsg["S-PatientVisitSegment_4"]["C-AssignedPatientLocation_4_03"]["E-PointOfCare_4_03.01"]}',
        }

        patient_info["msg_dt"] = date_utils.from_hldtime_to_dtime(
            patient_info["msg_dt"]
        )
        return patient_info

    def _is_valid_message(self, patient_info: dict) -> bool:
        """Validates if the message is worth processing based on event type and document type.

        Args:
            patient_info (dict): The extracted patient information.

        Returns:
            bool: True if the message is valid, False otherwise.
        """
        event_type_code = patient_info["event_type_code"]
        document_type = patient_info["document_type"]
        dept = patient_info["dept"]

        return event_type_code in ["T02", "T08", "T10"] and (
            document_type == "3049000006"
            or (document_type == "4" and dept not in self.icu_dept_list)
        )

    def enrich(self, dbm, data: dict) -> dict:
        """Enriches the input data with patient demographics.

        Args:
            dbm: The database manager to query patient data.
            data (dict): The data containing patient information.

        Returns:
            dict: The enriched data including patient demographics.
        """
        enriched_data = {"uid": data["uid"]}
        mrn = data["mrn"]
        msg_dtime_str = data["msg_dt"]

        enriched_data["trigger_datetime"] = msg_dtime_str
        enriched_data["mrn"] = data["mrn"]
        enriched_data["text"] = data["content"]

        # Query demographics
        demographics = self._get_patient_demographics(
            dbm, mrn, data["uid"], msg_dtime_str
        )
        enriched_data.update(demographics)

        logger.debug(json.dumps(enriched_data, indent=4))
        return enriched_data

    def _get_patient_demographics(
        self, dbm, mrn: str, uid: str, msg_dtime_str: str
    ) -> dict:
        """Fetches the patient's demographics from the database.

        Args:
            dbm: The database manager to query patient data.
            mrn (str): The patient's medical record number.
            uid (str): unique identifier
            msg_dtime_str (str): message datetime in string format

        Returns:
            dict: A dictionary with the patient's demographics.
        """
        query = """
            SELECT TOP 1 dob, gender, mrn 
            FROM patient_demographics 
            WHERE mrn = %s AND gender IS NOT NULL AND dob IS NOT NULL
        """
        result = dbm.execute_to_dataframe(query, [mrn])

        if result is None:
            raise Exception(
                f"Unable to retrieve patient demographics for UID {uid}. {log_utils.get_crying_sign()}"
            )

        dob = result["dob"].iloc[0].strftime(date_utils.DT_FORMAT)
        gender = result["gender"].iloc[0]

        age = (
            0
            if gen_utils.is_empty(dob)
            else date_utils.diff_year_str(dob, msg_dtime_str)
        )

        return {"age": age, "gender": self._parse_gender(gender)}

    def _parse_gender(self, gender) -> str:
        """Parses the gender value into a standard format.

        Args:
            gender: The gender value from the database.

        Returns:
            str: 'M' for male, 'F' for female, or the original value if unrecognized.
        """
        try:
            gender = int(gender)
            return "M" if gender == 1 else "F"
        except ValueError:
            return gender[0].upper()

    def predict(self, data: dict, metadata: dict = {}) -> dict:
        """Makes a prediction based on the provided data.

        Args:
            data (dict): The data for which to make a prediction.
            metadata (dict): Metadata to accompany the prediction.

        Returns:
            dict: The prediction result with relevant metadata.
        """
        try:
            prediction, attention_weights, words = self.model.predict(
                data["text"], data["age"], data["gender"]
            )

            metadata.update(
                {"text": words, "attention": attention_weights, "uid": data["uid"]}
            )

            logger.debug("Prediction result:")
            logger.debug(prediction)

            return {
                "result_prediction": {
                    "predictor": self.project_name,
                    "result": prediction,
                    "mrn": data["mrn"],
                    "alert_tier": 3,
                    "alert_trigger": 3,
                    "metadata": json.dumps(metadata),
                }
            }
        except Exception as error:
            logger.debug(data)
            logger.error(exception_utils.get_exception_string(error))
            logger.error(f"Prediction failed. {log_utils.get_crying_sign()}")
        return None
