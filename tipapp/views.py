"""Django project views."""


# Third-Party
import os
import logging
import io
import zipfile

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
from tipapp.tasks import get_files_list, get_file_record, get_thermal_files
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


def zip_directory_in_memory(full_path: str) -> io.BytesIO:
    """
    Zips a directory and returns it as an in-memory BytesIO object.
    This avoids creating temporary files on disk.
    """
    # Create an in-memory binary stream
    buffer = io.BytesIO()

    # Use a 'with' statement to ensure the zip file is properly finalized (closed)
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(full_path):
            for file in files:
                file_path = os.path.join(root, file)
                # Calculate the relative path to be used inside the zip archive
                arcname = os.path.relpath(file_path, start=full_path)
                zf.write(file_path, arcname)

    # Rewind the buffer's position to the beginning
    buffer.seek(0)
    
    return buffer


@api_view(["GET"])
@permission_classes([IsInAdminOrOfficersGroup])
def api_download_thermal_file_or_folder(request, *args, **kwargs):
    """
    Handles the download of a specified file or a zipped folder.

    This view validates that the requested path is within the allowed storage
    directories (DATA_STORAGE or UPLOADS_HISTORY_PATH) to prevent security vulnerabilities
    like path traversal.
    """
    # Log the initial request for debugging purposes.
    logger.info(f"Download request received from user: {request.user} for path: {request.GET.get('file_path')}")
    
    # Get the path provided by the frontend.
    user_provided_path = request.GET.get('file_path', '')

    # --- Secure Path Resolution and Validation ---

    # 1. Define a list of safe, absolute base directories from settings.
    #    These are the only top-level directories this view is allowed to serve files from.
    allowed_base_paths = [
        os.path.abspath(settings.DATA_STORAGE),
        os.path.abspath(settings.UPLOADS_HISTORY_PATH)
    ]

    # 2. Normalize the user-provided path to resolve any '..' components and get the real absolute path.
    #    For example, '/app/data/../data/files' becomes '/app/data/files'.
    target_path = os.path.abspath(user_provided_path)

    # 3. CRITICAL SECURITY CHECK:
    #    Verify that the normalized `target_path` starts with one of the allowed base paths.
    #    The 'any()' function checks if this condition is true for at least one item in the list.
    is_safe_path = any(target_path.startswith(base_path) for base_path in allowed_base_paths)
    
    if not is_safe_path:
        # If the check fails, it's a potential security risk (path traversal attack).
        logger.warning(
            f"Download attempt for an unsafe path was blocked. User: {request.user}, Requested Path: '{user_provided_path}'"
        )
        return JsonResponse({'error': 'Access to the specified path is denied.'}, status=403) # 403 Forbidden

    # --- Path Existence Check ---
    if not os.path.exists(target_path):
        logger.warning(f"Download request for a non-existent path: {target_path}")
        return JsonResponse({'error': 'File or folder not found.'}, status=404)

    # --- File/Folder Processing ---
    try:
        # Get the final filename for the download.
        download_filename = os.path.basename(target_path.rstrip(os.sep))

        if os.path.isdir(target_path):
            # --- Logic for FOLDER download ---
            logger.info(f"Target is a directory. Zipping in memory: {target_path}")
            
            # Use the helper function to zip the directory in memory.
            file_buffer = zip_directory_in_memory(target_path)
            
            # Create a FileResponse with the zipped content.
            response = FileResponse(file_buffer, as_attachment=True, filename=f"{download_filename}.zip")
            response["Content-Type"] = "application/zip"
            
        else: # The path points to a single FILE
            # --- Logic for FILE download ---
            logger.info(f"Target is a file. Serving directly: {target_path}")
            
            # Open the file in binary read mode.
            file_handle = open(target_path, 'rb')
            
            # Create a FileResponse with the file content.
            response = FileResponse(file_handle, as_attachment=True, filename=download_filename)
            response["Content-Type"] = "application/octet-stream"

        # This header is required for the frontend to be able to read the filename from the response.
        response["Access-Control-Expose-Headers"] = "Content-Disposition"
        
        logger.info(f"Successfully prepared download for {response.filename}")
        return response

    except Exception as e:
        # Catch any unexpected errors during zipping or file reading.
        logger.exception(f"An unexpected error occurred while preparing download for path {target_path}: {e}")
        return JsonResponse({'error': 'An internal error occurred while preparing the download.'}, status=500)


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