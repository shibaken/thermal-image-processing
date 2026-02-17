"""Thermal Image Processing URL Configuration.

The `urlpatterns` list routes URLs to views.
For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/

Examples:
    Function views
        1. Add an import:  from my_app import views
        2. Add a URL to urlpatterns:  path("", views.home, name="home")
    Class-based views
        1. Add an import:  from other_app.views import Home
        2. Add a URL to urlpatterns:  path("", Home.as_view(), name="home")
    Including another URLconf
        1. Import the include() function: from django.urls import include, path
        2. Add a URL to urlpatterns:  path("blog/", include("blog.urls"))
"""


# Third-Party
from django import conf
from django import urls
from django.contrib import admin
from django.contrib.auth import views as auth_views


# Local
from tipapp import views
# from govapp.apps.accounts.views import FileDeleteView, FileDownloadView, FileListView
# from govapp.default_data_manager import DefaultDataManager


# Admin Site Settings
admin.site.site_header = conf.settings.PROJECT_TITLE
admin.site.index_title = conf.settings.PROJECT_TITLE
admin.site.site_title = conf.settings.PROJECT_TITLE

# To test sentry
def trigger_error(request):
    division_by_zero = 1 / 0  # noqa

# Django URL Patterns
urlpatterns = [
    # Home Page
    urls.path("", views.ThermalFilesDashboardView.as_view(), name="home"),
    urls.path("files-dashboard", views.ThermalFilesDashboardView.as_view(), name="files-dashboard"),
    urls.path("upload-files", views.ThermalFilesUploadView.as_view(), name="upload-files"),
    urls.path("upload-monitor", views.UploadMonitorView.as_view(), name="upload-monitor"),
    urls.path("uploads-history", views.UploadsHistoryView.as_view(), name="uploads-history"),
    
    # Phase 6: Processing Jobs Dashboard
    urls.path("processing-jobs-dashboard", views.ProcessingJobsDashboardView.as_view(), name="processing-jobs-dashboard"),
    
    urls.path("api/upload-files/thermal_files/", views.api_upload_thermal_files),
    urls.path("api/upload-files/list_pending_imports/", views.list_pending_imports),
    urls.path("api/upload-files/api_delete_thermal_file/", views.api_delete_thermal_file),
    urls.path("api/thermal-files/list_thermal_folder_contents/", views.list_thermal_folder_contents),
    urls.path("api/thermal-files/list_uploaded_files/", views.list_uploads_history_contents),
    urls.path("api/thermal-files/download/", views.api_download_thermal_file_or_folder),
    
    # Phase 5: Job Monitoring API Endpoints
    urls.path("api/processing-jobs/", views.list_processing_jobs, name="list_processing_jobs"),
    urls.path("api/processing-jobs/<int:job_id>/", views.get_job_status, name="get_job_status"),
    # Django Administration
    urls.path("admin/", admin.site.urls),

    urls.path('api/trigger-long-command/', views.trigger_long_running_command_view, name='trigger_long_command'),
    urls.path('management-command-runner/', views.management_command_runner_page_view, name='management_command_runner'),
]

# DBCA Template URLs
urlpatterns.append(urls.path("logout/", auth_views.LogoutView.as_view(), {"next_page": "/"}, name="logout"))
if conf.settings.ENABLE_DJANGO_LOGIN:
    urlpatterns.append(urls.re_path(r"^ssologin/", auth_views.LoginView.as_view(), name="ssologin"))

# if not are_migrations_running():
#     DefaultDataManager()