"""Django project views."""


# Third-Party
import os
import logging

from django import http
from django import shortcuts
from django.http import JsonResponse, FileResponse
from django.views.generic import base
from django.contrib import auth
from django.core.paginator import Paginator
from rest_framework.decorators import permission_classes, api_view


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
    dir_path = settings.DATA_STORAGE
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

    dir_path = settings.DATA_STORAGE
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
