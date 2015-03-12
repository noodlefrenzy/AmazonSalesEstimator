using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.DataFactories.Runtime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AmazonDataTransforms
{
    public class AmazonMetadata
    {
        public AmazonMetadata()
        {
            this.AsinProps = new StringBuilder();
            this.Reviews = new StringBuilder();
            this.CategoriesById = new Dictionary<long, string>();
            this.AsinCategories = new Dictionary<string, HashSet<long>>();
            this.SimilarAsins = new StringBuilder();
        }

        public StringBuilder AsinProps { get; set; }
        public StringBuilder Reviews { get; set; }
        public Dictionary<long, string> CategoriesById { get; set; }
        public Dictionary<string, HashSet<long>> AsinCategories { get; set; }
        public StringBuilder SimilarAsins { get; set; }
    }

    public class ConvertAmazonMeta : IDotNetActivity
    {
        public const string AsinProductTableName = "AsinProductData";
        public const string AsinReviewTableName = "AsinReviewData";
        public const string AsinCategories = "AsinCategoryData";
        public const string SimilarAsinTableName = "SimilarAsinData";
        public const string CategoryTableName = "CategoryData";

        public IDictionary<string, string> Execute(IEnumerable<ResolvedTable> inputTables, 
            IEnumerable<ResolvedTable> outputTables, 
            IDictionary<string, string> extendedProperties, 
            IActivityLogger logger)
        {
            logger.Write(TraceEventType.Information, "Before anything...");

            logger.Write(TraceEventType.Information, "Printing dictionary entities if any...");
            foreach (KeyValuePair<string, string> entry in extendedProperties)
            {
                logger.Write(TraceEventType.Information, "<key:{0}> <value:{1}>", entry.Key, entry.Value);
            }

            var inputData = new StringBuilder();
            foreach (ResolvedTable inputTable in inputTables)
            {
                string connectionString = GetConnectionString(inputTable.LinkedService);
                string folderPath = GetFolderPath(inputTable.Table);

                if (String.IsNullOrEmpty(connectionString) ||
                    String.IsNullOrEmpty(folderPath))
                {
                    continue;
                }

                logger.Write(TraceEventType.Information, "Reading blob from: {0}", folderPath);

                CloudStorageAccount inputStorageAccount = CloudStorageAccount.Parse(connectionString);
                CloudBlobClient inputClient = inputStorageAccount.CreateCloudBlobClient();

                BlobContinuationToken continuationToken = null;

                do
                {
                    BlobResultSegment result = inputClient.ListBlobsSegmented(folderPath,
                                                true,
                                                BlobListingDetails.Metadata,
                                                null,
                                                continuationToken,
                                                null,
                                                null);
                    foreach (IListBlobItem listBlobItem in result.Results)
                    {
                        CloudBlockBlob inputBlob = listBlobItem as CloudBlockBlob;
                        int count = 0;
                        if (inputBlob != null)
                        {
                            using (StreamReader sr = new StreamReader(inputBlob.OpenRead()))
                            {
                                while (!sr.EndOfStream)
                                {
                                    string line = sr.ReadLine();
                                    if (count == 0)
                                    {
                                        logger.Write(TraceEventType.Information, "First line: [{0}]", line);
                                    }
                                    inputData.AppendLine(line);
                                    count++;
                                }

                            }

                        }
                    }
                    continuationToken = result.ContinuationToken;

                } while (continuationToken != null);
            }

            var data = ConvertData(inputData.ToString());

            foreach (ResolvedTable outputTable in outputTables)
            {
                string connectionString = GetConnectionString(outputTable.LinkedService);
                string folderPath = GetFolderPath(outputTable.Table);

                if (String.IsNullOrEmpty(connectionString) ||
                    String.IsNullOrEmpty(folderPath))
                {
                    continue;
                }

                logger.Write(TraceEventType.Information, "Writing blob to: {0}", folderPath);

                CloudStorageAccount outputStorageAccount = CloudStorageAccount.Parse(connectionString);
                Uri outputBlobUri = new Uri(outputStorageAccount.BlobEndpoint, folderPath + "/" + Guid.NewGuid() + ".csv");

                CloudBlockBlob outputBlob = new CloudBlockBlob(outputBlobUri, outputStorageAccount.Credentials);
                string output = null;
                switch (outputTable.Table.Name)
                {
                case ConvertAmazonMeta.AsinProductTableName:
                    output = data.AsinProps.ToString();
                    break;

                case ConvertAmazonMeta.AsinReviewTableName:
                    output = data.Reviews.ToString();
                    break;

                case ConvertAmazonMeta.CategoryTableName:
                    {
                        var catstr = new StringBuilder();
                        foreach (var kv in data.CategoriesById)
                        {
                            catstr.AppendLine(string.Format("{0},{1}", kv.Key, kv.Value));
                        }
                        output = catstr.ToString();
                    }
                    break;

                case ConvertAmazonMeta.SimilarAsinTableName:
                    output = data.SimilarAsins.ToString();
                    break;

                case ConvertAmazonMeta.AsinCategories:
                    {
                        var catstr = new StringBuilder();
                        foreach (var kv in data.AsinCategories)
                        {
                            foreach (var cid in kv.Value)
                            {
                                catstr.AppendLine(string.Format("{0},{1}", kv.Key, cid));
                            }
                        }
                        output = catstr.ToString();
                    }
                    break;

                default:
                    logger.Write(TraceEventType.Error, "Unknown table type: {0}", outputTable.Table.Name);
                    break;
                }

                if (output != null) 
                    outputBlob.UploadText(output);
            }
            return new Dictionary<string, string>();
        }

        private static string GetConnectionString(LinkedService asset)
        {
            AzureStorageLinkedService storageAsset;
            if (asset == null)
            {
                return null;
            }

            storageAsset = asset.Properties as AzureStorageLinkedService;
            if (storageAsset == null)
            {
                return null;
            }

            return storageAsset.ConnectionString;
        }

        private static string GetFolderPath(Table dataArtifact)
        {
            AzureBlobLocation blobLocation;
            if (dataArtifact == null || dataArtifact.Properties == null)
            {
                return null;
            }

            blobLocation = dataArtifact.Properties.Location as AzureBlobLocation;
            if (blobLocation == null)
            {
                return null;
            }

            return blobLocation.FolderPath;
        }

        /// <summary>
        /// Id:   0
        /// ASIN: 0771044445
        ///     discontinued product
        /// </summary>
        public static Regex DiscontinuedProductRegex = new Regex(@"\s*Id:\s+(?<id>[0-9]+)\s+" +
            @"ASIN:\s+(?<asin>\w+)\s+" +
            "discontinued product",
            RegexOptions.Multiline | RegexOptions.Compiled);

        public static Regex ProductRegex = new Regex(@"\s*Id:\s+(?<id>[0-9]+)\s+"+
            @"ASIN:\s+(?<asin>\w+)\s+"+
            @"title:\s+(?<title>[^\r\n]+)\s+"+
            @"group:\s+(?<group>[^\r\n]+)\s+"+
            @"salesrank:\s+(?<salesrank>[^\r\n]+)\s+"+
            @"similar:\s+(?<numsim>[0-9]+)\s+(?<sim>[^\r\n]+)\s+"+
            @"categories:\s+(?<numcat>[0-9]+)\s+"+
            @"(?<cats>(.|[\r\n])+)"+
            @"reviews:\s+total:\s+(?<totrev>[0-9]+)\s+downloaded:\s+(?<dlrev>[0-9]+)\s+avg rating:\s+(?<avgrev>[^\r\n]+)"+
            @"(?<revs>(.|[\r\n])+)", 
            RegexOptions.Multiline | RegexOptions.Compiled);

        /// <summary>
        ///    |Books[283155]|Subjects[1000]|Religion & Spirituality[22]|Christianity[12290]|Clergy[12360]|Preaching[12368]
        /// </summary>
        public static Regex CategoryRegex = new Regex(@"(?<catname>[^[]+)\[(?<catid>[0-9]+)\]", RegexOptions.Compiled);

        /// <summary>
        ///     2000-7-28  cutomer: A2JW67OY8U6HHK  rating: 5  votes:  10  helpful:   9
        /// </summary>
        public static Regex ReviewRegex = new Regex(@"(?<date>[0-9]+\-[0-9]+\-[0-9]+)\s+cus?tomer:\s+(?<custid>\w+)\s+rating:\s+(?<rating>[0-9]+)\s+votes:\s+(?<votes>[0-9]+)\s+helpful:\s+(?<helpful>[0-9]+)", RegexOptions.Compiled);

        public static void ParseProduct(string productData, AmazonMetadata meta)
        {
            var isDiscontinued = DiscontinuedProductRegex.Match(productData);
            if (isDiscontinued.Success)
            {
                Trace.TraceInformation("Discontinued: {0},{1}", isDiscontinued.Groups["id"].Value, isDiscontinued.Groups["asin"].Value);
                return;
            }

            var match = ProductRegex.Match(productData);
            if (match.Success)
            {
                var asin = match.Groups["asin"].Value;
                var props = string.Format("{0},{1},{2},{3},{4},{5},{6}", 
                    match.Groups["id"].Value, 
                    asin,
                    match.Groups["title"].Value,
                    match.Groups["group"].Value,
                    match.Groups["salesrank"].Value,
                    match.Groups["totrev"].Value,
                    match.Groups["avgrev"].Value);
                meta.AsinProps.AppendLine(props);

                var sims = match.Groups["sim"].Value.Split((char[])null, StringSplitOptions.RemoveEmptyEntries).ToArray();
                foreach (var sim in sims)
                {
                    meta.SimilarAsins.AppendLine(
                        string.Format("{0},{1}", asin, sim));
                }

                meta.AsinCategories[asin] = new HashSet<long>();

                var cats = match.Groups["cats"].Value.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(x => x.Trim()).ToArray();
                foreach (var cat in cats)
                {
                    var hierarchy = cat.Split('|');
                    foreach (var hier in hierarchy)
                    {
                        var catMatch = CategoryRegex.Match(hier);
                        if (catMatch.Success)
                        {
                            var catid = long.Parse(catMatch.Groups["catid"].Value);
                            var catname = catMatch.Groups["catname"].Value;
                            meta.CategoriesById[catid] = catname;
                            meta.AsinCategories[asin].Add(catid);
                        }
                    }
                }

                var revs = match.Groups["revs"].Value.Split(new char[]{ '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(x => x.Trim()).ToArray();
                foreach (var rev in revs)
                {
                    var revMatch = ReviewRegex.Match(rev);
                    if (revMatch.Success)
                    {
                        var date = DateTime.Parse(revMatch.Groups["date"].Value);
                        meta.Reviews.AppendLine(
                            string.Format("{0},{1},{2},{3},{4}", 
                            date.ToString("yyyy-MM-dd"), 
                            revMatch.Groups["custid"].Value, 
                            revMatch.Groups["rating"].Value, 
                            revMatch.Groups["votes"].Value, 
                            revMatch.Groups["helpful"].Value));
                    }
                }
            }
        }

        public static AmazonMetadata ConvertData(string inputData)
        {
            var meta = new AmazonMetadata();
            var inProduct = false;
            var curProduct = new StringBuilder();
            foreach (var line in inputData.Split('\n'))
            {
                var trimmed = line.Trim();
                if (trimmed.StartsWith("#")) continue;
                if (trimmed.Length == 0)
                {
                    inProduct = false;
                    if (curProduct.Length > 0)
                    {
                        ParseProduct(curProduct.ToString(), meta);
                        curProduct.Clear();
                    }
                } else if (inProduct)
                {
                    curProduct.AppendLine(trimmed);
                } else if (trimmed.StartsWith("Id:"))
                {
                    inProduct = true;
                    curProduct.AppendLine(trimmed);
                }
            }

            return meta;
        }
    }
}
