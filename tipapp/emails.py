import os
import decouple
import logging
from typing import List

from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.conf import settings

logger = logging.getLogger(__name__)


class ThermalProcessingEmailSender:
    """
    A service class dedicated to sending emails related to thermal image processing.
    """
    
    # --- Static templates for different notification types ---
    _SUBJECT_SUCCESS = "Thermal Image Processing Completed Successfully"
    _SUBJECT_FAILURE = "URGENT: Thermal Image Processing Failed"
    _SUBJECT_STARTED = "Thermal Image Processing Started"

    _TEMPLATE_SUCCESS_HTML = "emails/processing_success.html"
    _TEMPLATE_SUCCESS_TXT = "emails/processing_success.txt"
    
    _TEMPLATE_FAILURE_HTML = "emails/processing_failure.html"
    _TEMPLATE_FAILURE_TXT = "emails/processing_failure.txt"

    _TEMPLATE_STARTED_HTML = "emails/processing_started.html"
    _TEMPLATE_STARTED_TXT = "emails/processing_started.txt"

    def _get_recipient_list(self) -> List[str]:
        """
        Retrieves and parses the recipient list from environment variables.
        Returns a list of email addresses.
        """
        # Read the comma-separated string from the environment variable
        recipients_str = decouple.config("NOTIFICATION_RECIPIENTS", default="")
        
        # Split the string by commas and strip any whitespace from each address
        if recipients_str:
            return [email.strip() for email in recipients_str.split(',')]
        
        # Return an empty list if the variable is not set
        return []

    def _send_email(self, subject: str, context: dict, template_html: str, template_txt: str):
        """
        A generic internal method to build and send an email.
        """
        recipients = self._get_recipient_list()
        
        if not recipients:
            # You might want to log this as a warning
            # logger.warning("NOTIFICATION_RECIPIENTS is not set. Email not sent.")
            logging.warning("WARNING: NOTIFICATION_RECIPIENTS is not set. Email not sent.")
            return

        # Render the text and HTML content from templates
        text_content = render_to_string(template_txt, context)
        html_content = render_to_string(template_html, context)
        
        # Use the default 'from' address from settings.py
        from_email = settings.DEFAULT_FROM_EMAIL
        
        # Create the email message object
        msg = EmailMultiAlternatives(subject, text_content, from_email, recipients)
        
        # Attach the HTML version
        msg.attach_alternative(html_content, "text/html")
        
        # Send the email. Django will use the configured EMAIL_BACKEND.
        msg.send()
        
        logging.info(f"Email with subject '{subject}' sent to: {', '.join(recipients)}")

    def send_processing_started_notification(self, flight_name: str):
        """
        Sends a notification that a processing job has started.
        """
        context = {
            'flight_name': flight_name,
        }
        self._send_email(
            subject=self._SUBJECT_STARTED,
            context=context,
            template_html=self._TEMPLATE_STARTED_HTML,
            template_txt=self._TEMPLATE_STARTED_TXT
        )

    def send_success_notification(self, flight_name: str, details_message: str):
        """
        Sends a notification when the processing is successfully completed.
        """
        context = {
            'flight_name': flight_name,
            'details_message': details_message,
            # 'results_url': 'https://your-app.com/results/...'
        }
        self._send_email(
            subject=self._SUBJECT_SUCCESS,
            context=context,
            template_html=self._TEMPLATE_SUCCESS_HTML,
            template_txt=self._TEMPLATE_SUCCESS_TXT
        )

    def send_failure_notification(self, flight_name: str, error_message: str):
        """
        Sends a notification when the processing has failed.
        """
        context = {
            'flight_name': flight_name,
            'error_message': error_message,
        }
        self._send_email(
            subject=self._SUBJECT_FAILURE,
            context=context,
            template_html=self._TEMPLATE_FAILURE_HTML,
            template_txt=self._TEMPLATE_FAILURE_TXT
        )
