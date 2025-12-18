"""Django project views."""


# Third-Party
import os
import logging
import io

from django import http
from django import shortcuts
from django.http import JsonResponse, FileResponse
from django.views.generic import base
from django.contrib import auth
from django.core.paginator import Paginator
from rest_framework.decorators import permission_classes, api_view
from contextlib import redirect_stdout
from django.core.management import call_command
from django.views.decorators.http import require_POST
from django.contrib.auth.decorators import login_required, user_passes_test


# Internal
from tipapp import settings
from tipapp.tasks import get_files_list, get_file_record, get_thermal_files, zip_thermal_file_or_folder
from tipapp.permissions import IsInAdministratorsGroup, IsInAdminOrOfficersGroup, IsInOfficersGroup

# Typing
from typing import Any

logger = logging.getLogger(__name__)

UserModel = auth.get_user_model()


class HomePage(base.TemplateView):
    """Home page view."""

    template_name = "govapp/home.html"

    def get(self, request: http.HttpRequest, *args: Any, **kwargs: Any) -> http.HttpResponse:
        context: dict[str, Any] = {}
        return shortcuts.render(request, self.template_name, context)   


class ThermalFilesDashboardView(base.TemplateView):
    """Thermal files view."""
    template_name = "govapp/thermal-files/dashboard.html"

    def get(self, request: http.HttpRequest, *args: Any, **kwargs: Any) -> http.HttpResponse:
        context: dict[str, Any] = {
            "route_path": settings.DATA_STORAGE,
            'has_permission': IsInAdminOrOfficersGroup().has_permission(request, self),
        }
        return shortcuts.render(request, self.template_name, context)

class ThermalFilesUploadView(base.TemplateView):
    """Thermal files upload."""

    # Template name
    template_name = "govapp/thermal-files/upload-files.html"

    def get(self, request: http.HttpRequest, *args: Any, **kwargs: Any) -> http.HttpResponse:
        # Construct Context
        context: dict[str, Any] = {
            'has_view_permission': IsInAdminOrOfficersGroup().has_permission(request, self),
            'has_upload_permission': IsInAdministratorsGroup().has_permission(request, self),
        }

        return shortcuts.render(request, self.template_name, context)


class UploadsHistoryView(base.TemplateView):
    """Thermal files uploaded after processing."""

    # Template name
    template_name = "govapp/thermal-files/uploads-history.html"

    def get(self, request: http.HttpRequest, *args: Any, **kwargs: Any) -> http.HttpResponse:
        # Construct Context
        context: dict[str, Any] = {
            "route_path": settings.UPLOADS_HISTORY_PATH,
            'has_permission': IsInAdminOrOfficersGroup().has_permission(request, self),
        }
        return shortcuts.render(request, self.template_name, context)


@api_view(["GET"])
@permission_classes([IsInAdminOrOfficersGroup])
def list_pending_imports(request, *args, **kwargs):
    pathToFolder = settings.PENDING_IMPORT_PATH
    file_list = get_files_list(pathToFolder, ['.pdf', '.zip', '.7z'])
    page_param = request.GET.get('page', 1)
    page_size_param = request.GET.get('page_size', 10)
    paginator = Paginator(file_list, page_size_param)
    page = paginator.page(page_param)
   
    return JsonResponse({
        "count": paginator.count,
        "hasPrevious": page.has_previous(),
        "hasNext": page.has_next(),
        'results': page.object_list,
    })


@api_view(["GET"])
@permission_classes([IsInAdminOrOfficersGroup])
def list_thermal_folder_contents(request, *args, **kwargs):
    """
    Safely lists the contents of a directory within the DATA_STORAGE path.
    Prevents path traversal attacks.
    """
    # --- Input parameters from the request ---
    page_param = request.GET.get('page', '1')
    page_size_param = request.GET.get('page_size', '10')
    search_term = request.GET.get('search', '')
    
    # The path provided by the user/frontend, relative to the data storage root.
    relative_path_from_user = request.GET.get('route_path', '')

    # 1. Define the absolute, trusted base directory from settings.
    base_dir = os.path.abspath(settings.DATA_STORAGE)

    # 2. Safely join the base directory with the user-provided relative path.
    target_path = os.path.join(base_dir, relative_path_from_user.lstrip('/\\'))

    # 3. Normalize the resulting path to resolve any '..' components (e.g., '/app/data/../data/files' -> '/app/data/files').
    final_abs_path = os.path.abspath(target_path)

    # 4. CRITICAL SECURITY CHECK:
    #    Verify that the final, resolved path is still inside (or is the same as) our safe base directory.
    if not final_abs_path.startswith(base_dir):
        logger.warning(
            f"Path traversal attempt blocked. User: {request.user}, "
            f"Requested Path: '{relative_path_from_user}'"
        )
        return JsonResponse({'error': 'Access denied: Invalid path.'}, status=403) # 403 Forbidden is more appropriate
    
    # --- Check if the validated path exists ---
    if not os.path.exists(final_abs_path) or not os.path.isdir(final_abs_path):
        return JsonResponse({'error': f"Directory not found: '{relative_path_from_user}'"}, status=404)

    # --- Retrieve and paginate the file list ---
    try:
        # Pass the safe, absolute path to the function that gets the files.
        file_list = get_thermal_files(final_abs_path, int(page_param) - 1, int(page_size_param), search_term)
        
        paginator = Paginator(file_list, page_size_param)
        page = paginator.page(page_param)
        
        return JsonResponse({
            "count": paginator.count,
            "hasPrevious": page.has_previous(),
            "hasNext": page.has_next(),
            'results': page.object_list,
        })
    except Exception as e:
        logger.error(f"Error retrieving file list for '{final_abs_path}': {e}", exc_info=True)
        return JsonResponse({'error': 'An error occurred while retrieving the file list.'}, status=500)


@api_view(["GET"])
@permission_classes([IsInAdminOrOfficersGroup])
def list_uploads_history_contents(request, *args, **kwargs):
    dir_path = settings.UPLOADS_HISTORY_PATH
    page_param = request.GET.get('page', '1')
    page_size_param = request.GET.get('page_size', '10')
    route_path = request.GET.get('route_path', '')
    search = request.GET.get('search', '')

    dir_path = route_path if route_path.startswith(dir_path) else os.path.join(dir_path, route_path)

    if not os.path.exists(dir_path):
        return JsonResponse({'error': f'Path [{dir_path}] does not exist.'}, status=400)

    file_list = get_thermal_files(dir_path, int(page_param) - 1, int(page_size_param), search)
    paginator = Paginator(file_list, page_size_param)
    page = paginator.page(page_param)
    return JsonResponse({
        "count": paginator.count,
        "hasPrevious": page.has_previous(),
        "hasNext": page.has_next(),
        'results': page.object_list,
    })

@api_view(["POST"])
@permission_classes([IsInAdministratorsGroup])
def api_upload_thermal_files(request, *args, **kwargs):
    if request.FILES:
        # uploaded_files = []  # Multiple files might be uploaded
        allowed_extensions = ['.zip', '.7z', '.pdf']
        uploaded_file = request.FILES.getlist('file')[0]
        newFileName = request.POST.get('newFileName', '')

        logger.info(f'File: [{uploaded_file.name}] is being uploaded...')

        # Check file extensions
        _, file_extension = os.path.splitext(uploaded_file.name)
        if file_extension.lower() not in allowed_extensions:
            return JsonResponse({'error': 'Invalid file type. Only .zip and .7z files are allowed.'}, status=400)

        # Save files
        save_path = os.path.join(settings.PENDING_IMPORT_PATH,  newFileName)
        with open(save_path, 'wb+') as destination:
            for chunk in uploaded_file.chunks():
                destination.write(chunk)
        logger.info(f"File: [{uploaded_file.name}] has been successfully saved at [{save_path}].")
        file_info = get_file_record(settings.PENDING_IMPORT_PATH, newFileName)
        return JsonResponse({'message': 'File(s) uploaded successfully.', 'data' : file_info})
    else:
        logger.info(f"No file(s) were uploaded.")
        return JsonResponse({'error': 'No file(s) were uploaded.'}, status=400)

@api_view(["POST"])
@permission_classes([IsInAdministratorsGroup])
def api_delete_thermal_file(request, *args, **kwargs):
    file_name = request.data.get('newFileName', '')
    file_path = os.path.join(settings.PENDING_IMPORT_PATH, file_name)
    if file_name != '' and os.path.exists(file_path):
        os.remove(file_path)
        return JsonResponse({'message': f'File [{file_name}] has been deleted successfully.'})
    else:
        return JsonResponse({'error': f'File [{file_name}] does not exist.'}, status=400)


@api_view(["GET"])
@permission_classes([IsInAdminOrOfficersGroup])
def api_download_thermal_file_or_folder(request, *args, **kwargs):
    logger.info(f"Download request received from user: {request.user} for path: {request.GET.get('file_path')}")

    target_path = request.GET.get('file_path', '')

    file_path = target_path
    
    # Normalize the path to handle mixed slashes correctly
    file_path = os.path.normpath(file_path)

    # Check if the path exists
    if file_path != '' and os.path.exists(file_path):
        try:
            file_to_serve = None
            download_filename = ""

            # Check if it is a directory or a file
            if os.path.isdir(file_path):
                # CASE 1: Directory -> Zip it
                download_file_path = zip_thermal_file_or_folder(file_path)
                
                if download_file_path and os.path.exists(download_file_path):
                    file_to_serve = download_file_path
                    # Use folder name + .zip
                    original_name = os.path.basename(file_path.rstrip(os.sep))
                    download_filename = f"{original_name}.zip"
                    logger.info(f"Directory zipped successfully. Serving: {file_to_serve} as {download_filename}")
                else:
                    logger.error(f"Failed to zip directory or zipped file not found: {file_path}")
                    return JsonResponse({'error': 'Error zipping folder.'}, status=500)
            else:
                logger.info(f"Target is a file: {file_path}. Serving directly.")
                # CASE 2: File -> Serve directly (no zip)
                file_to_serve = file_path
                # Use original filename
                download_filename = os.path.basename(file_path)

            # Serve the file if we have a valid path and filename
            if file_to_serve and download_filename:
                logger.debug(f"Opening file handle for: {file_to_serve}")
                file_handle = open(file_to_serve, 'rb')
                
                response = FileResponse(file_handle, as_attachment=True, filename=download_filename)
                
                # Set headers
                response["Content-Type"] = "application/octet-stream"
                response["Content-Disposition"] = f'attachment; filename="{download_filename}"'
                response["Access-Control-Expose-Headers"] = "Content-Disposition"
                
                logger.info(f"Successfully prepared download for {download_filename}") 
                return response
            else:
                logger.error(f"Download preparation failed: file_to_serve={file_to_serve}, download_filename={download_filename}")
                return JsonResponse({'error': 'Error preparing download.'}, status=500)
            
        except FileNotFoundError as e:
            logger.error(f"File not found during serving process: {e}", exc_info=True)
            return JsonResponse({'error': 'File not found on server.'}, status=404)
        except Exception as e:
            # Log the error with stack trace for detailed debugging
            logger.exception(f"Error serving file/folder for path {file_path}: {e}")
            return JsonResponse({'error': 'Error preparing download.'}, status=500) 
    else:
        if file_path == '':
            logger.warning(f"Download request received with empty 'file_path' parameter from user: {request.user}")
        else:
            logger.warning(f"File or folder not found at path: {file_path}")
        return JsonResponse({'error': 'File or folder not found.'}, status=400)


def is_staff_user(user):
    return user.is_staff


@login_required
@user_passes_test(is_staff_user)
@require_POST
def trigger_long_running_command_view(request):
    """
    Directly runs a long-running management command and waits for it to complete.
    Designed for admin users who are aware of the long wait time.
    """
    try:
        # Create an in-memory text buffer to capture the output (print statements) of the management command.
        command_output_buffer = io.StringIO()

        # Use a context manager to temporarily redirect all standard output (stdout) to the in-memory buffer.
        with redirect_stdout(command_output_buffer):
            # Execute management command
            call_command('process_imported_files_command')
        
        # Get the entire captured output as a string.
        command_output = command_output_buffer.getvalue()

        # Return a successful response
        return JsonResponse({
            'status': 'success',
            'message': 'Command executed successfully.',
            'output': command_output
        })

    except Exception as e:
        logger.error(f"Error running management command: {e}", exc_info=True)
        
        return JsonResponse({
            'status': 'error',
            'message': f'The command failed with an error: {str(e)}'
        }, status=500)


@login_required
@user_passes_test(is_staff_user)
def management_command_runner_page_view(request):
    """
    Renders the HTML page that contains the button to run the management command.
    Its only job is to display the template.
    """
    template_name = "govapp/management_command_runner.html"
    context = {
        'page_title': 'Management Command Runner'
    }
    
    return shortcuts.render(request, template_name, context)