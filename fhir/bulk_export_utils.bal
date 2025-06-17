import ballerina/time;
import ballerina/file;
import ballerina/ftp;
import ballerina/http;
import ballerina/io;
import ballerina/log;
import ballerina/task;

// configurable BulkExportServerConfig sourceServerConfig = ?;
// configurable BulkExportClientConfig clientServiceConfig = ?;
// configurable TargetServerConfig targetServerConfig = ?;

final isolated map<ExportTask> exportTasks = {};

isolated function addExportTasktoMemory(map<ExportTask> taskMap, ExportTask exportTask) returns boolean {
    // add the export task to the memory
    exportTask.lastUpdated = time:utcNow();
    lock {
        taskMap[exportTask.id] = exportTask;
    }
    return true;
}

isolated function addPollingEventToMemory(map<ExportTask> taskMap, PollingEvent pollingEvent) returns boolean {
    // add the polling event to the memory
    ExportTask exportTask = taskMap.get(pollingEvent.id);
    exportTask.lastUpdated = time:utcNow();
    exportTask.lastStatus = pollingEvent.exportStatus ?: "In-progress";
    lock {
        taskMap.get(pollingEvent.id).pollingEvents.push(pollingEvent);
    }
    return true;
}

isolated function updateExportTaskStatusInMemory(map<ExportTask> taskMap, string exportTaskId, string newStatus) returns boolean {

    ExportTask exportTask = taskMap.get(exportTaskId);
    exportTask.lastUpdated = time:utcNow();
    exportTask.lastStatus = newStatus;
    return true;
}

isolated function getExportTaskFromMemory(string exportId) returns ExportTask {
    // get the export task from the memory
    ExportTask exportTask;
    lock {
        exportTask = exportTasks.get(exportId).clone();
    }
    return exportTask;
}


isolated function executeJob(PollingTask job, decimal interval) returns task:JobId|error? {

    // Implement the job execution logic here
    task:JobId id = check task:scheduleJobRecurByFrequency(job, interval);
    job.setId(id);
    return id;
}

isolated function unscheduleJob(task:JobId id) returns error? {

    // Implement the job termination logic here
    log:printDebug("Unscheduling the job.", Jobid = id);
    task:Error? unscheduleJob = task:unscheduleJob(id);
    if unscheduleJob is task:Error {
        log:printError("Error occurred while unscheduling the job.", unscheduleJob);
    }
    return null;
}

isolated function getFileAsStream(string downloadLink, http:Client statusClientV2) returns stream<byte[], io:Error?>|error? {

    http:Response|http:ClientError statusResponse = statusClientV2->get("/");
    if statusResponse is http:Response {
        int status = statusResponse.statusCode;
        if status == 200 {
            return check statusResponse.getByteStream();
        } else {
            log:printError("Error occurred while getting the status.");
        }
    } else {
        log:printError("Error occurred while getting the status.", statusResponse);
    }
    return null;
}

isolated function saveFileInFS(string downloadLink, string fileName) returns error? {

    http:Client statusClientV2 = check new (downloadLink);
    stream<byte[], io:Error?> streamer = check getFileAsStream(downloadLink, statusClientV2) ?: new ();

    check io:fileWriteBlocksFromStream(fileName, streamer);
    check streamer.close();
    log:printDebug(string `Successfully downloaded the file. File name: ${fileName}`);
}

isolated function sendFileFromFSToFTP(TargetServerConfig config, string sourcePath, string fileName) returns error? {
    // Implement the FTP server logic here.
    ftp:Client fileClient = check new ({
        host: config.host,
        auth: {
            credentials: {
                username: config.username,
                password: config.password
            }
        }
    });
    stream<io:Block, io:Error?> fileStream
        = check io:fileReadBlocksAsStream(sourcePath, 1024);
    check fileClient->put(string `${config.directory}/${fileName}`, fileStream);
    check fileStream.close();
}

isolated function downloadFiles(json exportSummary, string exportId, string targetDirectory, TargetServerConfig targetServerConfig) returns error? {

    ExportSummary exportSummary1 = check exportSummary.cloneWithType(ExportSummary);

    foreach OutputFile item in exportSummary1.output {
        log:printDebug("Downloading the file.", url = item.url);
        error? downloadFileResult = saveFileInFS(item.url, string `${targetDirectory}${file:pathSeparator}${exportId}${file:pathSeparator}${item.'type}-exported.ndjson`);
        if downloadFileResult is error {
            log:printError("Error occurred while downloading the file.", downloadFileResult);
        }
        if targetServerConfig.'type == "ftp" {
            // download the file to the FTP server
            // implement the FTP server logic
            error? uploadFileResult = sendFileFromFSToFTP(targetServerConfig, string `${targetDirectory}${file:pathSeparator}${item.'type}-exported.ndjson`, string `${item.'type}-exported.ndjson`);
            if uploadFileResult is error {
                log:printError("Error occurred while sending the file to ftp.", downloadFileResult);

            }
        }
    }
    lock {
        boolean _ = updateExportTaskStatusInMemory(taskMap = exportTasks, exportTaskId = exportId, newStatus = "Downloaded");
    }
    log:printInfo("All files downloaded successfully.");
    return null;
}

isolated function addQueryParam(string queryString, string key, string value) returns string {
    if queryString == "" {
        return string `?${key}=${value}`;
    } else {
        return string `${queryString}&${key}=${value}`;
    }
}

class PollingTask {

    *task:Job;
    string exportId;
    string lastStatus;
    string location;
    string targetDirectory;
    TargetServerConfig targetServerConfig;
    task:JobId jobId = {id: 0};

    public function execute() {
        do {
            http:Client statusClientV2 = check new (self.location);

            log:printDebug("Polling the export task status.", exportId = self.exportId);
            if self.lastStatus == "In-progress" {
                // update the status
                // update the export task

                http:Response|http:ClientError statusResponse;
                statusResponse = statusClientV2->/;
                addPollingEvent addPollingEventFuntion = addPollingEventToMemory;
                if statusResponse is http:Response {
                    int status = statusResponse.statusCode;
                    if status == 200 {
                        // update the status
                        // extract payload
                        // unschedule the job
                        self.setLastStaus("Completed");
                        lock {
                            boolean _ = updateExportTaskStatusInMemory(taskMap = exportTasks, exportTaskId = self.exportId, newStatus = "Export Completed. Downloading files.");
                        }
                        json payload = check statusResponse.getJsonPayload();
                        log:printDebug("Export task completed.", exportId = self.exportId, payload = payload);
                        error? unscheduleJobResult = unscheduleJob(self.jobId);
                        if unscheduleJobResult is error {
                            log:printError("Error occurred while unscheduling the job.", unscheduleJobResult);
                        }

                        // download the files
                        error? downloadFilesResult = downloadFiles(payload, self.exportId, self.targetDirectory, self.targetServerConfig);
                        if downloadFilesResult is error {
                            log:printError("Error in downloading files", downloadFilesResult);
                        }

                    } else if status == 202 {
                        // update the status
                        log:printDebug("Export task in-progress.", exportId = self.exportId);
                        string progress = check statusResponse.getHeader("X-Progress");
                        PollingEvent pollingEvent = {id: self.exportId, eventStatus: "Success", exportStatus: progress};

                        lock {
                            // persisting event
                            boolean _ = addPollingEventFuntion(exportTasks, pollingEvent.clone());
                        }
                        self.setLastStaus("In-progress");
                    }
                } else {
                    log:printError("Error occurred while getting the status.", statusResponse);
                    lock {
                        // statusResponse
                        PollingEvent pollingEvent = {id: self.exportId, eventStatus: "Failed"};
                        boolean _ = addPollingEventFuntion(exportTasks, pollingEvent.cloneReadOnly());
                    }
                }
            } else if self.lastStatus == "Completed" {
                // This is a rare occurance; if the job is not unscheduled properly, it will keep polling the status.
                log:printDebug("Export task completed.", exportId = self.exportId);
            }
        } on fail var e {
            log:printError("Error occurred while polling the export task status.", e);
        }
    }

    isolated function init(string exportId, string location, string targetDirectory, TargetServerConfig targetServerConfig, string lastStatus = "In-progress") {
        self.exportId = exportId;
        self.lastStatus = lastStatus;
        self.location = location;
        self.targetDirectory = targetDirectory;
        self.targetServerConfig = targetServerConfig;
    }

    public function setLastStaus(string newStatus) {
        self.lastStatus = newStatus;
    }

    public isolated function setId(task:JobId jobId) {
        self.jobId = jobId;
    }
}

isolated function submitBackgroundJob(string taskId, http:Response|http:ClientError status, decimal defaultIntervalInSec, string targetDirectory, TargetServerConfig targetServerConfig) {
    if status is http:Response {
        log:printDebug(status.statusCode.toBalString());

        // get the location of the status check
        do {
            string location = check status.getHeader("Content-location");
            task:JobId|() _ = check executeJob(new PollingTask(taskId, location, targetDirectory, targetServerConfig), defaultIntervalInSec);
            log:printDebug("Polling location recieved: " + location);
        } on fail var e {
            log:printError("Error occurred while getting the location or scheduling the Job", e);
            // if location is available, can retry the task
        }
    } else {
        log:printError("Error occurred while sending the kick-off request to the bulk export server.", status);
    }
}
