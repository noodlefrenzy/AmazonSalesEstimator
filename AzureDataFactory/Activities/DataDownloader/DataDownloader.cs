namespace DataDownloaderActivityNS
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.IO.Compression;
    using System.Net;
    using System.Reflection;
    using Microsoft.Azure.Management.DataFactories.Models;
    using Microsoft.DataFactories.Runtime;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    public class DataDownloaderActivity : IDotNetActivity
    {
        private IActivityLogger _logger;

        public IDictionary<string, string> Execute(
            IEnumerable<ResolvedTable> inputTables,
            IEnumerable<ResolvedTable> outputTables,
            IDictionary<string, string> inputs,
            IActivityLogger activityLogger)
        {
            _logger = activityLogger;

            var url = inputs["url"];
            var fileName = inputs["fileName"];

            _logger.Write(TraceEventType.Information, "Gathering data: ..");

            // Temporary staging folder
            string dataStagingFolder = string.Format(@"{0}", Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));
            Directory.CreateDirectory(dataStagingFolder);

            // Temporary staging file
            string decompressedFile = Path.Combine(dataStagingFolder, fileName);

            TriggerRequest(url, decompressedFile);

            foreach (var outputTable in outputTables)
            {
                string connectionString = GetConnectionString(outputTable.LinkedService);
                string folderPath = GetFolderPath(outputTable.Table);

                UploadToBlobStorage(connectionString, folderPath, fileName, decompressedFile);
            }

            if (File.Exists(decompressedFile))
            {
                File.Delete(decompressedFile);
            }

            _logger.Write(TraceEventType.Information, "Exit");
            return new Dictionary<string, string>();
        }

        private void UploadToBlobStorage(string connectionString, string destinationFolder, string destinationFile, string sourcePath)
        {
            try
            {
                CloudStorageAccount outputStorageAccount = CloudStorageAccount.Parse(connectionString);

                _logger.Write(TraceEventType.Information, "Uploading to Blob: ..");
                Uri outputBlobUri = new Uri(outputStorageAccount.BlobEndpoint, destinationFolder + "/" + destinationFile + ".csv");

                CloudBlockBlob outputBlob = new CloudBlockBlob(outputBlobUri, outputStorageAccount.Credentials);
                outputBlob.UploadFromFile(sourcePath, FileMode.OpenOrCreate);
            }
            catch (Exception ex)
            {
                _logger.Write(TraceEventType.Error, "Error occurred : {0}", ex);
            }
        }

        private void TriggerRequest(string url, string decompressedFile)
        {
            int retries = 0;
            int maxRetries = 3;
            bool found = false;
            while (retries < maxRetries && !found)
            {
                try
                {
                    _logger.Write(TraceEventType.Information, "Making request to url : {0}..", url);

                    var request = (HttpWebRequest)WebRequest.Create(url);
                    using (var response = (HttpWebResponse)request.GetResponse())
                    {
                        using (var reader = new StreamReader(response.GetResponseStream()))
                        {
                            _logger.Write(TraceEventType.Information, "Decompressing to a file: ..");
                            using (FileStream decompressedFileStream = File.Create(decompressedFile))
                            {
                                using (var decompressionStream = new GZipStream(reader.BaseStream, CompressionMode.Decompress))
                                {
                                    decompressionStream.CopyTo(decompressedFileStream);
                                    _logger.Write(TraceEventType.Information, "Decompression complete to : {0}", decompressedFile);
                                }
                            }
                        }
                    }
                    found = true;
                }
                catch(Exception e)
                {
                    _logger.Write(TraceEventType.Warning, "Unable to download : {0} with error: {1}.", url, e.Message);
                }
                retries++;
            }

            if (retries == maxRetries)
            {
                _logger.Write(TraceEventType.Error, "Max Retries Hit");
            }
        }

        private static string GetConnectionString(LinkedService asset)
        {
            if (asset == null)
            {
                return null;
            }

            AzureStorageLinkedService storageAsset = asset.Properties as AzureStorageLinkedService;
            if (storageAsset == null)
            {
                return null;
            }

            return storageAsset.ConnectionString;
        }

        private static string GetFolderPath(Table dataArtifact)
        {
            if (dataArtifact == null || dataArtifact.Properties == null)
            {
                return null;
            }

            AzureBlobLocation blobLocation = dataArtifact.Properties.Location as AzureBlobLocation;
            if (blobLocation == null)
            {
                return null;
            }

            return blobLocation.FolderPath;
        }
    }
}
