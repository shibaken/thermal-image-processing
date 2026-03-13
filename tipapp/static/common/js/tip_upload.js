let tip_upload = {
  dt: null,
  var: {
    page: 1,
    page_size: 10,
    data: [],
    upload_files_url: "/api/upload-files/",
  },

  init: function () {
    const _ = tip_upload;
    _.var.page = 1;
    _.var.page_size = 10;

    _.dt = $("#pending-imports table").DataTable({
      serverSide: true,
      language: utils.datatable.common.language,
      ajax: function (data, callback, settings) {
        _.get_pending_imports_data(
          {
            page: data && data.start ? data.start / data.length + 1 : 1,
            page_size: data?.length,
            search: data?.search?.value,
            draw: data?.draw,
          },
          function (response) {
            const { count, results } = response;
            callback({
              data: results,
              recordsTotal: count,
              recordsFiltered: count,
            });
          },
          function (error) {}
        );
      },
      headerCallback: function (thead, data, start, end, display) {
        $(thead).addClass("table-dark");
      },
      columns: [{ data: "name" }, { data: "created_at" }],
    });

    const mainSelector = ".file-upload";
    const container = $(mainSelector);
    container
      .find('input[type="file"]')
      .on("change", (e) => tip_upload.handleFileInputChange(e));

    container.find(".btn.btn-primary").on("click", function () {
      document.querySelector(`${mainSelector} input[type="file"]`).click();
    });

    // Drag-and-drop events
    $(mainSelector).on("dragover", function (e) {
      e.preventDefault();
      e.stopPropagation();
      $(mainSelector).addClass("dragover");
    });
    $(mainSelector).on("dragenter", function (e) {
      e.preventDefault();
      e.stopPropagation();
      enterTarget = e.target;
      $(mainSelector).addClass("dragover");
    });
    $(mainSelector).on("dragleave", function (e) {
      if (enterTarget == e.target) {
        e.stopPropagation();
        e.preventDefault();
        $(mainSelector).removeClass("dragover");
        enterTarget = null;
      }
    });
    $(mainSelector).on("drop", this, function (e) {
      enterTarget = null;

      e.preventDefault();
      e.stopPropagation();
      $(this).removeClass("dragover");
      const ev = e.originalEvent;
      if (ev.dataTransfer.items) {
        tip_upload.uploadFiles(
          [...ev.dataTransfer.items]
            .map((item, i) => {
              if (item.kind === "file") {
                return item.getAsFile();
              }
            })
            .filter((f) => f)
        );
      } else {
        tip_upload.uploadFiles(ev.dataTransfer.files);
      }
    });
  },
  handleFileInputChange: (e) => {
    const files = $(e.target).prop("files");
    if (!files || files.length === 0) return;
    tip_upload.uploadFiles(files);
    // Reset the file input so the same file can be selected again
    e.target.value = '';
  },

  get_pending_imports_data: function (params, cb_success, cb_error) {
    const defaultParams = {
      page: params?.page ?? tip_upload.var.page,
      page_size: params?.page_size ?? tip_upload.var.page_size,
    };
    const _params = {
      name__icontains: $("#file-name").val(),
    };
    params_str = utils.make_query_params(
      Object.assign({}, _params, defaultParams ?? {})
    );

    $.ajax({
      url:
        tip_upload.var.upload_files_url + "list_pending_imports/?" + params_str,
      method: "GET",
      dataType: "json",
      contentType: "application/json",
      success: cb_success,
      error: cb_error,
    });
  },
  addDateTimeToFilename: function (filename) {
    // Get current date and time
    const currentDate = new Date();

    // Format date
    const year = currentDate.getFullYear();
    const month = String(currentDate.getMonth() + 1).padStart(2, "0"); // Pad month with leading zero
    const day = String(currentDate.getDate()).padStart(2, "0"); // Pad day with leading zero
    const hours = String(currentDate.getHours()).padStart(2, "0"); // Pad hours with leading zero
    const minutes = String(currentDate.getMinutes()).padStart(2, "0"); // Pad minutes with leading zero
    const seconds = String(currentDate.getSeconds()).padStart(2, "0"); // Pad seconds with leading zero

    // Insert date and time between filename and extension
    const extensionIndex = filename.lastIndexOf(".");
    const newFilename =
      filename.slice(0, extensionIndex) +
      "." +
      year +
      month +
      day +
      "_" +
      hours +
      minutes +
      seconds +
      filename.slice(extensionIndex);

    return newFilename;
  },

  createProgressBar: function (fileName, newFileName) {
    const { markup } = utils;
    console.log("creating progress bar");

    const progressBarContainer = markup("div", "", {
      class: "progress-container mt-2",
    });

    const progressBarRow = markup("div", "", {
      class: "row d-flex align-items-center",
    });
    const fileNameColumn = markup("div", "", { class: "col-6" });

    const progressBarColumn = markup("div", "", { class: "col-4" });
    const progressBarTextColumn = markup(
      "div",
      { tag: "span", class: "progress-text" },
      { class: "col-1" }
    );
    const deleteIconColumn = markup("div", "", { class: "col-1" });
    const progressBar = markup(
      "div",
      {
        tag: "div",
        class: "progress-bar",
        role: "progressbar",
        style: "width: 0%;",
        "aria-valuenow": "0",
        "aria-valuemin": "0",
        "aria-valuemax": "100",
      },
      { class: "progress" }
    );

    const fileNameElement = markup("div", fileName, { class: "file-name" });
    const deleteIcon = markup(
      "span",
      '<svg width="24" height="24" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M4 12L12 4" stroke="currentColor" stroke-width="2" stroke-linecap="round"/><path d="M12 12L4 4" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>',
      { class: "delete-icon", "data-newFileName": newFileName }
    );

    fileNameColumn.append(fileNameElement);
    progressBarColumn.append(progressBar);
    deleteIconColumn.append(deleteIcon);

    progressBarRow.append(fileNameColumn);
    progressBarRow.append(progressBarTextColumn);
    progressBarRow.append(progressBarColumn);
    progressBarRow.append(deleteIconColumn);

    progressBarContainer.append(progressBarRow);

    return {
      progressBar: $(progressBar),
      progressBarContainer: $(progressBarContainer),
      deleteIcon: $(deleteIcon),
    };
  },

  deleteFile: function (fileName) {
    // Make an AJAX request to delete the file
    let csrf_token = $("#csrfmiddlewaretoken").val();
    let xhr = $.ajax({
      url: tip_upload.var.upload_files_url + "api_delete_thermal_file/", // Change the URL to your delete file endpoint
      type: "POST",
      headers: { "X-CSRFToken": csrf_token }, // Include CSRF token in headers
      data: { newFileName: fileName },
      success: function (data) {},
      error: function (xhr, status, error) {
        console.error("Error deleting file:", error);
      },
    });
  },

  cancelUpload: function () {
    $(".cross-sign").each(function () {
      let newFilename = $(this).data("newfilename");
      tip_upload.deleteFile(newFilename);
    });
    let progressBarContainer = $("#progressBars");
    progressBarContainer.empty();
  },

  // Function for uploading files
  uploadFiles: function (files) {
    let xhrList = [];
    let completed = 0;
    let csrf_token = $("#csrfmiddlewaretoken").val();

    for (let i = 0; i < files.length; i++) {
      let fileName = files[i].name;
      let newFileName = tip_upload.addDateTimeToFilename(fileName);

      // Generate progressbar per file
      let { progressBar, progressBarContainer, deleteIcon } =
        tip_upload.createProgressBar(fileName, newFileName);
      $("#progressBars").append(progressBarContainer);

      (function (index, progressBar, progressBarContainer) {
        const formData = new FormData();
        formData.append("file", files[index]);
        formData.append("newFileName", newFileName);

        deleteIcon.on("click", function () {
          if (xhrList[index] && xhrList[index].readyState !== 4) {
            xhrList[index].abort();
            return;
          } else {
            const newFileName = $(this).attr("data-new-file-name");
            tip_upload.deleteFile(newFileName);
          }
          progressBarContainer.fadeOut("slow", function () {
            $(this).remove();
          });
        });

        // Upload
        const xhr = $.ajax({
          url: tip_upload.var.upload_files_url + "thermal_files/",
          type: "POST",
          headers: { "X-CSRFToken": csrf_token },
          data: formData,
          cache: false,
          contentType: false,
          processData: false,
          xhr: function () {
            const xhr = new window.XMLHttpRequest();
            xhr.upload.addEventListener(
              "progress",
              function (evt) {
                if (evt.lengthComputable) {
                  const percentComplete = (evt.loaded / evt.total) * 100;
                  // Update progressbar
                  progressBar
                    .find(".progress-bar")
                    .width(percentComplete + "%");
                  progressBar
                    .find(".progress-bar")
                    .attr("aria-valuenow", percentComplete);
                  // Display percentage text
                  progressBarContainer
                    .find(".progress-text")
                    .text(percentComplete.toFixed(0) + "%");

                  if (percentComplete === 100) {
                    // Change progressbar color to green
                    deleteIcon.remove()
                    progressBar
                    .find(".progress-bar")
                    .removeClass("bg-info")
                    .addClass("bg-success");
                    setTimeout(() => {
                      const spinner = utils.markup(
                        "div",
                        [ 
                          utils.markup("div", "", {
                          class: "spinner-border spinner-border-sm spinn text-success",
                          role: "status",
                        }), {tag: "span", content: "Processing file", class: "text-success"}],
                        { class: "d-flex gap-2 w-100" }
                      )
                      $(spinner).insertBefore(progressBar);
                      progressBar.remove();
                      
                    }, 1000)
                    
                  }
                }
              },
              false
            );
            return xhr;
          },
          success: function (response) {
            progressBarContainer.fadeOut("slow", function () {
              $(this).remove();
            });
            if (++completed === xhrList.length) {
              tip_upload?.dt.draw(["page"]);
            }
          },
          error: function (xhr, status, error) {
            if (++completed === xhrList.length) {
              tip_upload?.dt.draw(["page"]);
            }
            if (!xhr.responseText) return;
            let errorResponse = JSON.parse(xhr.responseText);
            // progressBar may already be removed from the DOM (replaced by the
            // spinner when progress hit 100%). Target the column directly instead.
            progressBarContainer.find(".col-4").empty().append(
              $('<span class="error-message"></span>').text(errorResponse.error)
            );
            progressBarContainer.find(".progress-text").empty();
            // Inject a dismiss button into the icon column.
            const dismissBtn = $(
              '<span class="delete-icon" title="Dismiss">' +
              '<svg width="24" height="24" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">' +
              '<path d="M4 12L12 4" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>' +
              '<path d="M12 12L4 4" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>' +
              '</svg></span>'
            );
            dismissBtn.on("click", function () {
              progressBarContainer.fadeOut("slow", function () {
                $(this).remove();
              });
            });
            progressBarContainer.find(".col-1").last().empty().append(dismissBtn);
          },
        });
        xhrList.push(xhr);
      })(i, progressBar, progressBarContainer);
    }
  },
};
