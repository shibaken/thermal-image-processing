from django.conf import settings
from wagov_utils.components.utils.emails import TemplateEmailBase
import logging

logger = logging.getLogger(__name__)


class ProcessingStartedEmail(TemplateEmailBase):
    """
    An email to notify that the thermal processing has started.
    """
    subject = "Thermal Image Processing Started"
    html_template = "emails/processing_started.html"
    txt_template = "emails/processing_started.txt"

class ProcessingSuccessEmail(TemplateEmailBase):
    """
    An email to notify that the thermal processing was successful.
    """
    subject = "Thermal Image Processing Completed Successfully"
    html_template = "emails/processing_success.html"
    txt_template = "emails/processing_success.txt"

class ProcessingFailureEmail(TemplateEmailBase):
    """
    An email to notify that the thermal processing has failed.
    """
    subject = "URGENT: Thermal Image Processing Failed"
    html_template = "emails/processing_failure.html"
    txt_template = "emails/processing_failure.txt"

# --- Internal helper to reduce code duplication ---
def _send_notification(email_class, context):
    """
    A private helper function to handle the common logic of sending an email.
    It instantiates the email class, gets recipients, sends the email, and logs the result.
    """
    recipients = getattr(settings, 'NOTIFICATION_RECIPIENTS', [])
    
    if not recipients:
        logger.warning(f"NOTIFICATION_RECIPIENTS is not set in settings. Email with subject '{email_class.subject}' will not be sent.")
        return

    try:
        # Instantiate the email class
        email = email_class()
        
        # The 'send' method from TemplateEmailBase returns the message object on success, or None on failure.
        msg = email.send(recipients, context=context)
        
        # --- LOGGING ---
        if msg:
            # Successfully sent
            logger.info(f"Successfully sent notification email with subject '{email.subject}' to: {recipients}")
        else:
            # The 'send' method returned None, indicating a failure handled within the library.
            logger.error(f"Failed to send notification email with subject '{email.subject}' to: {recipients}. Check wagov_utils email log for details.")
    except Exception as e:
        # Catch any other unexpected exceptions during the process.
        logger.error(f"An unexpected error occurred while trying to send email with subject '{email_class.subject}': {e}", exc_info=True)


# --- Public helper functions to be called from your application ---
def send_processing_started_notification(flight_name: str):
    """
    Prepares and sends the 'Processing Started' email.
    """
    context = {
        'flight_name': flight_name,
    }
    _send_notification(ProcessingStartedEmail, context)


def send_success_notification(flight_name: str, details_message: str):
    """
    Prepares and sends the 'Success' email.
    """
    context = {
        'flight_name': flight_name,
        'details_message': details_message,
    }
    _send_notification(ProcessingSuccessEmail, context)


def send_failure_notification(flight_name: str, error_message: str):
    """
    Prepares and sends the 'Failure' email.
    """
    context = {
        'flight_name': flight_name,
        'error_message': error_message,
    }
    _send_notification(ProcessingFailureEmail, context)
