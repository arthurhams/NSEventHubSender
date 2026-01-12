using System.Text;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace NSEventHubSender;

public class NSEventHubSender
{
    private readonly ILogger<NSEventHubSender> _logger;

    public NSEventHubSender(ILogger<NSEventHubSender> logger)
    {
        _logger = logger;
    }

    [Function("NSEventHubSender")]
    public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
    {
        _logger.LogInformation("C# HTTP trigger function processed a request.");

        try
        {
            // Get EventHub connection string from environment variables
            var connectionString = Environment.GetEnvironmentVariable("EventHubIngestConnectionString");
            var eventHubName = Environment.GetEnvironmentVariable("EventHubIngestName");

            if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(eventHubName))
            {
                return new BadRequestObjectResult("EventHub configuration is missing. Please set EventHubConnectionString and EventHubName in local.settings.json");
            }

            // Create a producer client to send events to EventHub
            await using var producerClient = new EventHubProducerClient(connectionString, eventHubName, new DefaultAzureCredential());


            // Create a batch of events
            int messageSize = 10; // in Kbytes
            try
            {
                if (req.Query.ContainsKey("messageSize"))
                    messageSize = int.Parse(req.Query["messageSize"]!);
            }
            catch
            {
                _logger.LogWarning("Invalid messageSize parameter. Using default value of 10.");
            }

            string trainId = "T0001";
            if (req.Query.ContainsKey("trainId") && !string.IsNullOrEmpty(req.Query["trainId"]) && !string.IsNullOrWhiteSpace(req.Query["trainId"]))
                trainId = req.Query["trainId"];

            // Read message from request body or use default
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var message = string.IsNullOrEmpty(requestBody) ? CreateMessageOfSize(messageSize, trainId) : requestBody;

            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();


            // Create a batch of events
            int batchSize = 10; // in Kbytes
            try
            {
                if (req.Query.ContainsKey("batchSize"))
                    batchSize = int.Parse(req.Query["batchSize"]!);
            }
            catch
            {
                _logger.LogWarning("Invalid batchSize parameter. Using default value of 10.");
            }


            int numberOfEvents = 10; // Number of events to send
            try
            {
                if (req.Query.ContainsKey("numberOfEvents") && !string.IsNullOrEmpty(req.Query["numberOfEvents"]) && !string.IsNullOrWhiteSpace(req.Query["numberOfEvents"]))
                    numberOfEvents = int.Parse(req.Query["numberOfEvents"]);

            }
            catch
            {
                _logger.LogWarning("Invalid numberOfEvents parameter. Using default value of 10.");
            }

            int currentNumberOfEvents = 0;
            while (currentNumberOfEvents < batchSize)
            {
                for (int i = 1; i <= batchSize; i++)
                {
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes($"Event {i}")));
                    currentNumberOfEvents++;
                }
                // Send the batch of events
                await producerClient.SendAsync(eventBatch);
                eventBatch.Dispose();
            }
            _logger.LogInformation($"Successfully sent message to EventHub: {message}");

            return new OkObjectResult(new
            {
                status = "success",
                message = "Event sent to EventHub successfully",
                eventData = message
            });
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error sending message to EventHub: {ex.Message}");
            return new StatusCodeResult(StatusCodes.Status500InternalServerError);
        }
    }

    //function that create a string with the size of the give parameter in kb
    private string CreateMessageOfSize(int sizeInKb, string trainId = "T0001")
    {
        var sb = new StringBuilder();
        for (int i = 0; i < sizeInKb * 64; i++)
        {
            sb.Append(string.Concat(trainId, "M", i.ToString("D4"), ":", Random.Shared.NextInt64(0, 9999).ToString("D4"), ";"));
        }
        return sb.ToString();
    }
}
