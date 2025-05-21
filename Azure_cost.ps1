param($Timer) # Input binding for Timer trigger (if used)
# --- Configurable Delays (in seconds) ---
# Adjust these values if 429 errors persist.
$delayForCostTypeQuerySec = 5     # Delay between ActualCost and AmortizedCost queries for the same subscription
$delayBetweenSubscriptionsSec = 5  # Delay between processing each subscription
# --- New Retry Parameters for API Queries ---
$script:maxQueryRetries = 3          # Maximum number of retries for a single API query
$script:initialQueryRetryDelaySec = 5 # Initial delay in seconds for exponential backoff if Retry-After is not provided
# Helper Function to get Managed Identity Token
function Get-ManagedIdentityAccessToken {
    param (
        [string]$Resource = "https://management.azure.com/"
    )
    $apiVersion = "2019-08-01" # MSI API version
    $tokenAuthUri = $env:MSI_ENDPOINT + "?resource=$Resource&api-version=$apiVersion"
    $tokenResponse = Invoke-RestMethod -Method Get -Headers @{ "X-IDENTITY-HEADER" = $env:MSI_SECRET } -Uri $tokenAuthUri
    return $tokenResponse.access_token
}
# Updated function to get both Actual and Amortized costs using Cost Management Query API with Retry Logic
function Get-SubscriptionActualAndAmortizedCosts {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [string]
        $SubscriptionId,
        [Parameter(Mandatory)]
        [string]
        $BillingPeriodName, # e.g., "202310"
        [Parameter(Mandatory)]
        [string]
        $AccessToken,
        [Parameter(Mandatory)]
        [int]
        $DelayBetweenQueriesInSeconds, # This is your $delayForCostTypeQuerySec passed from the main script
        # Default to script-scoped global configuration for retries
        [int]$MaxRetries = $script:maxQueryRetries,
        [int]$InitialRetryDelay = $script:initialQueryRetryDelaySec
    )
    $costs = [PSCustomObject]@{
        ActualCost    = "Error" # Default to Error, will be updated on success
        AmortizedCost = "Error" # Default to Error, will be updated on success
        Currency      = "N/A"
    }
    try {
        $StartDate = [datetime]::parseexact($BillingPeriodName, 'yyyyMM', $null)
        $QueryStartDate = $StartDate.ToString("yyyy-MM-ddT00:00:00Z")
        $QueryEndDate = $StartDate.AddMonths(1).AddDays(-1).ToString("yyyy-MM-ddT23:59:59Z")
        $queryApiUri = "https://management.azure.com/subscriptions/$($SubscriptionId)/providers/Microsoft.CostManagement/query?api-version=2023-11-01"
        $costTypes = @("ActualCost", "AmortizedCost")
        foreach ($costType in $costTypes) {
            Write-Verbose "Querying $costType for Subscription '$SubscriptionId', Billing Period '$BillingPeriodName'"
            $requestBodyContent = @{
                type       = $costType
                timeframe  = "Custom"
                timePeriod = @{
                    from = $QueryStartDate
                    to   = $QueryEndDate
                }
                dataset    = @{
                    granularity = "None"
                    aggregation = @{
                        totalCost = @{
                            name     = "Cost"
                            function = "Sum"
                        }
                    }
                }
            } | ConvertTo-Json -Depth 5
            $irmCostParams = @{
                Method      = 'Post'
                Uri         = $queryApiUri
                Headers     = @{ Authorization = "Bearer $AccessToken"; "Content-Type" = "application/json" }
                Body        = $requestBodyContent
                ErrorAction = 'Stop' # Crucial for the catch block to trigger on HTTP errors
            }
            $currentCostValue = "Error" # Initialize for this cost type attempt, will be overwritten on success
            for ($attempt = 1; $attempt -le ($MaxRetries + 1); $attempt++) {
                try {
                    $response = Invoke-RestMethod @irmCostParams
                    if ($response -and $response.properties -and $response.properties.rows -and $response.properties.rows.Count -gt 0) {
                        $costValueFromApi = $response.properties.rows[0][0]
                        $currencyValueFromApi = $response.properties.rows[0][1]
                        $currentCostValue = [decimal]$costValueFromApi 
                        if ($costs.Currency -eq "N/A" -and $currencyValueFromApi) {
                            $costs.Currency = $currencyValueFromApi
                        }
                        Write-Verbose "$costType for Sub '$SubscriptionId': $currentCostValue $($costs.Currency) (Attempt $attempt successful)"
                        break # Success, exit retry loop for this costType
                    } else {
                        Write-Warning "No data returned from API for $costType for subscription '$SubscriptionId' on attempt $attempt. Treating as 0 cost for now."
                        $currentCostValue = [decimal]0.0 
                        if ($response -and $response.properties -and $response.properties.rows -and $response.properties.rows.Count -gt 0 -and $response.properties.rows[0].Count -gt 1) {
                             $currencyValueFromApi = $response.properties.rows[0][1]
                             if ($costs.Currency -eq "N/A" -and $currencyValueFromApi) {
                                $costs.Currency = $currencyValueFromApi
                             }
                        }
                        break 
                    }
                }
                catch {
                    $exception = $_.Exception
                    $actualStatusCode = $null # Renamed for clarity and re-evaluated each catch
                    if ($exception -is [Microsoft.PowerShell.Commands.HttpResponseException] -and $null -ne $exception.Response) {
                        try {
                            $actualStatusCode = [int]$exception.Response.StatusCode # Cast HttpStatusCode enum to int
                            if ($null -eq $actualStatusCode) {
                                Write-Warning "Casting HttpStatusCode enum to [int] resulted in null for $costType on sub '$SubscriptionId'. Original enum value: '$($exception.Response.StatusCode)'"
                            }
                        } catch {
                            Write-Warning "CRITICAL: Could not cast exception.Response.StatusCode ('$($exception.Response.StatusCode)') to [int] for $costType on sub '$SubscriptionId'. Error: $($_.Exception.Message)"
                        }
                    } else {
                        Write-Warning "Exception for $costType on sub '$SubscriptionId' was not Microsoft.PowerShell.Commands.HttpResponseException or its Response object was null. Actual exception type: '$($exception.GetType().FullName)'. Cannot determine HTTP status code for retry logic."
                    }
                    Write-Warning "Attempt $attempt for $costType on sub '$SubscriptionId' failed. Derived ActualStatusCode: '$actualStatusCode'. Full Exception Message: $($exception.Message)"
                    if ($attempt -gt $MaxRetries) {
                        Write-Error "Max retries ($MaxRetries) reached for $costType on subscription '$SubscriptionId'. Cost will be marked as 'Error'."
                        if ($exception.Response) {
                            try {
                                $errorStream = $exception.Response.GetResponseStream()
                                $reader = New-Object System.IO.StreamReader($errorStream)
                                $errorBody = $reader.ReadToEnd()
                                $reader.Close(); $errorStream.Close()
                                Write-Error "Final Error Response Body for $costType query on sub '$SubscriptionId' after max retries: $errorBody"
                            } catch { Write-Warning "Could not read final error response body for sub '$SubscriptionId'."}
                        }
                        break # Exit retry loop, $currentCostValue remains "Error"
                    }
                    if ($actualStatusCode -eq 429) {
                        Write-Host "Rate limit (429) detected. ActualStatusCode: $actualStatusCode. Proceeding with retry logic for $costType on sub '$SubscriptionId'."
                        $delaySeconds = $InitialRetryDelay 
                        if ($exception.Response.Headers.AllKeys -contains "Retry-After") {
                            $retryAfterValue = $exception.Response.Headers["Retry-After"]
                            if ([int]::TryParse($retryAfterValue, [ref]$parsedDelay) -and $parsedDelay -gt 0) {
                                $delaySeconds = $parsedDelay
                                Write-Host "Retry-After header found: $retryAfterValue seconds for $costType on sub '$SubscriptionId'."
                            } else {
                                Write-Warning "Could not parse Retry-After header value '$retryAfterValue' for sub '$SubscriptionId'. Using exponential backoff: $InitialRetryDelay * 2^($($attempt - 1))."
                                $delaySeconds = $InitialRetryDelay * ([Math]::Pow(2, ($attempt - 1)))
                            }
                        } else {
                            Write-Warning "No Retry-After header. Using exponential backoff: $InitialRetryDelay * 2^($($attempt -1)) for $costType on sub '$SubscriptionId'."
                            $delaySeconds = $InitialRetryDelay * ([Math]::Pow(2, ($attempt - 1))) 
                        }
                        
                        $minJitterDelay = [Math]::Max(1, [int]($delaySeconds * 0.8)) 
                        $maxJitterDelay = [Math]::Max($minJitterDelay + 1, [int]($delaySeconds * 1.2) + 1) 
                        $actualSleepDelay = Get-Random -Minimum $minJitterDelay -Maximum $maxJitterDelay
                        
                        Write-Warning "Rate limit (429) hit for $costType on subscription '$SubscriptionId'. Retrying attempt $($attempt + 1) of $($MaxRetries +1) after $actualSleepDelay seconds..."
                        Start-Sleep -Seconds $actualSleepDelay
                        # Continue to the next iteration of the for loop (retry)
                    } else {
                        Write-Error "Non-retryable error or status code not reliably identified as 429. ActualStatusCode: '$actualStatusCode'. Querying $costType for subscription '$SubscriptionId'. Exception: $($exception.Message). Cost will be marked 'Error'."
                        if ($exception.Response) {
                             try {
                                $errorStream = $exception.Response.GetResponseStream()
                                $reader = New-Object System.IO.StreamReader($errorStream)
                                $errorBody = $reader.ReadToEnd()
                                $reader.Close(); $errorStream.Close()
                                Write-Error "Error Response Body for non-retryable error on $costType query for sub '$SubscriptionId': $errorBody"
                            } catch { Write-Warning "Could not read error response body for non-retryable error on sub '$SubscriptionId'."}
                        }
                        break # Exit retry loop, $currentCostValue remains "Error"
                    }
                } # End Catch
            } # End For Retry Loop
            if ($costType -eq "ActualCost") {
                $costs.ActualCost = $currentCostValue
            } elseif ($costType -eq "AmortizedCost") {
                $costs.AmortizedCost = $currentCostValue
            }
            if ($costType -eq "ActualCost" -and $costTypes.Count -gt 1) {
                Write-Verbose "Sleeping for $DelayBetweenQueriesInSeconds seconds before next cost type query (AmortizedCost) for subscription '$SubscriptionId'..."
                Start-Sleep -Seconds $DelayBetweenQueriesInSeconds
            }
        } # End Foreach CostType
    }
    catch {
        Write-Error "General error in Get-SubscriptionActualAndAmortizedCosts for subscription '$SubscriptionId' (Billing Period: $BillingPeriodName): $($_.Exception.Message)"
        Write-Error $_.ScriptStackTrace
    }
    return $costs
}
# Function Send-FileToSlack (Unchanged from your provided script)
function Send-FileToSlack {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$SlackBotToken,
        [Parameter(Mandatory)]
        [string]$ChannelId,
        [Parameter(Mandatory)]
        [string]$FilePath, 
        [Parameter(Mandatory)]
        [string]$OriginalFileName, 
        [string]$InitialComment = "Here is the Azure Cost Report.",
        [string]$FileContentType = "text/csv" 
    )
    try {
        Write-Host "Send-FileToSlack: Initiated with OriginalFileName='$OriginalFileName', FilePath='$FilePath'."
        $slackMetadataFileName = "AzureCostReport.csv" 
        Write-Host "Send-FileToSlack: Using simplified metadata filename '$slackMetadataFileName' for getUploadURLExternal call."
        if (-not $OriginalFileName -or $OriginalFileName.Trim() -eq "") {
            Write-Error "Send-FileToSlack: OriginalFileName parameter is null, empty, or whitespace. Cannot proceed."
            return
        }
        Write-Host "Send-FileToSlack: OriginalFileName is '$OriginalFileName'."
        if (-not $FilePath -or $FilePath.Trim() -eq "") {
            Write-Error "Send-FileToSlack: FilePath parameter is null, empty, or whitespace. Cannot proceed."
            return
        }
        if (-not (Test-Path -LiteralPath $FilePath -PathType Leaf)) {
            Write-Error "Send-FileToSlack: File does not exist or is not a file at path: '$FilePath'."
            return 
        }
        
        $fileSizeBytes = $null 
        try {
            $fileInfo = Get-Item -LiteralPath $FilePath -ErrorAction Stop
            $fileSizeBytes = $fileInfo.Length
        } catch {
            Write-Error "Send-FileToSlack: Error getting file information for '$FilePath'. Exception: $($_.Exception.Message)"
            return
        }
        if ($null -eq $fileSizeBytes) { 
            Write-Error "Send-FileToSlack: FileSizeBytes is null for existing file '$FilePath'. This is unexpected."
            return 
        }
        if ($fileSizeBytes -lt 0) { 
             Write-Error "Send-FileToSlack: FileSizeBytes is negative ($fileSizeBytes) for '$FilePath'. This is invalid."
            return
        }
        Write-Host "Send-FileToSlack: FilePath is '$FilePath', FileSizeBytes is $fileSizeBytes (Type: $($fileSizeBytes.GetType().FullName))."
        Write-Host "Step 1: Getting Slack upload URL for file (metadata filename: '$slackMetadataFileName') using GET..."
        
        $encodedFileNameForGetUrl = [System.Web.HttpUtility]::UrlEncode($slackMetadataFileName)
        $uriForGetUrl = "https://slack.com/api/files.getUploadURLExternal?filename=$($encodedFileNameForGetUrl)&length=$($fileSizeBytes)"
        Write-Host "Debug - Slack Upload Step 1: URI for GET request: '$uriForGetUrl'"
        $irmParamsGetUrl = @{
            Uri         = $uriForGetUrl
            Method      = 'Get' 
            Headers     = @{ "Authorization" = "Bearer $SlackBotToken" }
            ErrorAction = 'Stop'
        }
        $uploadUrlResponse = Invoke-RestMethod @irmParamsGetUrl
        
        if (-not $uploadUrlResponse.ok) {
            Write-Error "Slack API Error (files.getUploadURLExternal with GET): $($uploadUrlResponse.error)"
            Write-Error "Full Slack Response (files.getUploadURLExternal with GET): $($uploadUrlResponse | ConvertTo-Json -Depth 3)"
            return
        }
        $uploadUrl = $uploadUrlResponse.upload_url
        $fileId = $uploadUrlResponse.file_id
        Write-Host "Successfully got upload URL. File ID: $fileId. Upload URL: $uploadUrl"
        Write-Host "Step 2: Uploading file content (from '$OriginalFileName') to pre-signed URL using POST multipart/form-data..."
        
        $form = @{
            filename = Get-Item -LiteralPath $FilePath 
        }
        $irmParamsUploadFile = @{
            Uri         = $uploadUrl
            Method      = 'Post' 
            Form        = $form 
            ErrorAction = 'Stop'
        }
        Invoke-RestMethod @irmParamsUploadFile 
        Write-Host "File content POSTed successfully to pre-signed URL."
        Write-Host "Step 3: Completing Slack file upload for File ID '$fileId' (actual filename: '$OriginalFileName') to channel '$ChannelId'..."
        
        $completePayloadHashtable = @{
            files           = @(@{ id = $fileId; title = $OriginalFileName }) 
            channel_id      = $ChannelId
            initial_comment = $InitialComment
        }
        $completeUploadJsonString = $completePayloadHashtable | ConvertTo-Json -Compress 
        Write-Host "Debug - Slack Upload Step 3: COMPRESSED JSON String for files.completeUploadExternal: '$completeUploadJsonString'"
        $utf8BodyBytesForComplete = [System.Text.Encoding]::UTF8.GetBytes($completeUploadJsonString)
        $irmParamsCompleteUpload = @{
            Uri         = "https://slack.com/api/files.completeUploadExternal"
            Method      = 'Post'
            Headers     = @{ "Authorization" = "Bearer $SlackBotToken" }
            Body        = $utf8BodyBytesForComplete 
            ContentType = "application/json; charset=utf-8" 
            ErrorAction = 'Stop'
        }
        $completeResponse = Invoke-RestMethod @irmParamsCompleteUpload
        if ($completeResponse.ok) {
            Write-Host "Successfully uploaded and shared '$OriginalFileName' to Slack channel '$ChannelId'."
        } else {
            Write-Error "Slack API Error (files.completeUploadExternal): $($completeResponse.error)"
            Write-Error "Full Slack Response (files.completeUploadExternal): $($completeResponse | ConvertTo-Json -Depth 3)"
        }
    }
    catch {
        Write-Error "Exception during Slack file upload process: $($_.Exception.Message)"
        Write-Error $_.ScriptStackTrace
        if ($_.Exception.Response) {
            try {
                $errorResponseStream = $_.Exception.Response.GetResponseStream()
                $streamReader = New-Object System.IO.StreamReader($errorResponseStream)
                $slackErrorBody = $streamReader.ReadToEnd()
                $streamReader.Close()
                $errorResponseStream.Close()
                Write-Error "Slack API Error Body (from exception): $slackErrorBody"
            } catch {
                Write-Warning "Could not read detailed error response body from Slack exception."
            }
        }
    }
}
# --- Main Azure Function Logic ---
Write-Host "Azure Function 'Get-PreviousMonthSubscriptionCosts' started."
if ($Timer) { Write-Host "Timer information: Last run: $($Timer.Last), Next run: $($Timer.ScheduleStatus.Next)" }
$VerbosePreference = "Continue"
$slackBotToken = $env:SLACK_BOT_TOKEN
$slackChannelId = $env:SLACK_CHANNEL_ID
if (-not $slackBotToken -or -not $slackChannelId) {
    Write-Warning "Slack Bot Token or Channel ID is not configured. Slack notification will be skipped."
}
try {
    Write-Host "Attempting to get Managed Identity Access Token..."
    $accessToken = Get-ManagedIdentityAccessToken -Resource "https://management.azure.com/"
    if (-not $accessToken) { Write-Error "Failed to obtain Access Token."; return }
    Write-Host "Successfully obtained Managed Identity Access Token."
    Write-Host "Fetching list of subscriptions..."
    $subscriptionsUri = "https://management.azure.com/subscriptions?api-version=2020-01-01"
    $subscriptionsResult = Invoke-RestMethod -Method Get -Uri $subscriptionsUri -Headers @{ Authorization = "Bearer $accessToken" }
    if (-not $subscriptionsResult.value) { Write-Warning "No subscriptions found."; return }
    $subscriptions = $subscriptionsResult.value
    Write-Host "Found $($subscriptions.Count) subscriptions."
    $currentDate = Get-Date 
    $previousMonthDate = $currentDate.AddMonths(-1)
    $billingPeriodName = $previousMonthDate.ToString('yyyyMM')
    Write-Host "Calculating costs for billing period: $billingPeriodName"
    $allSubscriptionCostsData = @()
    $subscriptionCounter = 0
    foreach ($sub in $subscriptions) {
        $subscriptionCounter++
        $subscriptionId = $sub.subscriptionId
        $subscriptionName = $sub.displayName
        Write-Host "Processing Subscription $subscriptionCounter of $($subscriptions.Count): '$subscriptionName' (ID: $subscriptionId)"
        
        $costDetails = Get-SubscriptionActualAndAmortizedCosts -SubscriptionId $subscriptionId `
                                                              -BillingPeriodName $billingPeriodName `
                                                              -AccessToken $accessToken `
                                                              -DelayBetweenQueriesInSeconds $delayForCostTypeQuerySec
        if ($costDetails) {
            $allSubscriptionCostsData += [PSCustomObject]@{
                SubscriptionName = $subscriptionName; SubscriptionId = $subscriptionId; BillingPeriod = $billingPeriodName
                ActualCost = if ($costDetails.ActualCost -is [decimal]) { [Math]::Round($costDetails.ActualCost, 2) } else { $costDetails.ActualCost }
                AmortizedCost = if ($costDetails.AmortizedCost -is [decimal]) { [Math]::Round($costDetails.AmortizedCost, 2) } else { $costDetails.AmortizedCost }
                Currency = $costDetails.Currency
            }
        } else {
            Write-Warning "Get-SubscriptionActualAndAmortizedCosts returned null for subscription '$subscriptionName' ($subscriptionId). This is unexpected."
            $allSubscriptionCostsData += [PSCustomObject]@{
                SubscriptionName = $subscriptionName; SubscriptionId = $subscriptionId; BillingPeriod = $billingPeriodName
                ActualCost = "Error retrieving"; AmortizedCost = "Error retrieving"; Currency = "N/A"
            }
        }
        if ($subscriptionCounter -lt $subscriptions.Count) { 
            Write-Verbose "Sleeping for $delayBetweenSubscriptionsSec seconds before next subscription..."
            Start-Sleep -Seconds $delayBetweenSubscriptionsSec
        }
    }
    Write-Host "--- Previous Month Subscription Costs Summary ($billingPeriodName) ---"
    if ($allSubscriptionCostsData.Count -gt 0) {
        Write-Host ($allSubscriptionCostsData | Format-Table -AutoSize | Out-String)
        if ($slackBotToken -and $slackChannelId) {
            $csvFileName = "Azure_Costs_$($billingPeriodName)_$($currentDate.ToString('yyyyMMddHHmmss')).csv" 
            $tempCsvPath = Join-Path -Path "/tmp" -ChildPath $csvFileName 
            Write-Host "Creating CSV file at $tempCsvPath..."
            try {
                if ($allSubscriptionCostsData.Count -eq 0) {
                    Write-Warning "No data to export to CSV for Slack upload."
                } else {
                    $allSubscriptionCostsData | Export-Csv -Path $tempCsvPath -NoTypeInformation -Encoding UTF8 -ErrorAction Stop
                
                    $slackSendParams = @{
                        SlackBotToken    = $slackBotToken
                        ChannelId        = $slackChannelId
                        FilePath         = $tempCsvPath
                        OriginalFileName = $csvFileName 
                        InitialComment   = "Azure Monthly Cost Report for $billingPeriodName"
                        FileContentType  = "text/csv"
                    }
                    Send-FileToSlack @slackSendParams
                }
            }
            catch {
                Write-Error "Error during CSV creation or Slack upload: $($_.Exception.Message)"
                Write-Error $_.ScriptStackTrace 
            }
            finally {
                if (Test-Path -LiteralPath $tempCsvPath -PathType Leaf) { 
                    Write-Host "Removing temporary CSV file: $tempCsvPath"
                    Remove-Item -Path $tempCsvPath -Force -ErrorAction SilentlyContinue
                }
            }
        } else {
            Write-Host "Slack integration not configured; skipping CSV generation and Slack notification."
        }
    } else {
        Write-Host "No subscription cost data processed or all subscriptions resulted in errors."
    }
}
catch {
    Write-Error "An unhandled error occurred in the main script: $($_.Exception.Message)"
    Write-Error $_.ScriptStackTrace
}
Write-Host "Azure Function 'Get-PreviousMonthSubscriptionCosts' finished."
