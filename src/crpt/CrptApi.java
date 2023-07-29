package crpt;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CrptApi {
    private static final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient client;
    private final String baseUrl = "https://markirovka.demo.crpt.tech";
    private String token;
    private final Limiter limiter;


    public CrptApi(TimeUnit interval, int requestLimit) {
        client = HttpClient.newHttpClient();
        limiter = new Limiter(requestLimit, 1, 100, 30, interval);
        token = getToken();
    }

    private enum DocumentFormat {
        MANUAL, XML, CSV
    }

    @JsonSerialize(using = RequestSerializer.class)
    private static class CreateDocumentRequest {

        private DocumentFormat documentFormat;
        private String productDocument;
        private String productGroup;
        private String signature;
        private String type;

        public CreateDocumentRequest(Document productDocument, String productGroup, String signature) throws JsonProcessingException {
            this.documentFormat = DocumentFormat.MANUAL;
            this.productDocument = mapper.writeValueAsString(productDocument);
            this.productGroup = productGroup;
            this.signature = signature;
            this.type = "LP_INTRODUCE_GOODS";
        }

        public CreateDocumentRequest() {
        }

        public DocumentFormat getDocumentFormat() {
            return documentFormat;
        }

        public void setDocumentFormat(DocumentFormat documentFormat) {
            this.documentFormat = documentFormat;
        }

        public String getProductDocument() {
            return productDocument;
        }

        public void setProductDocument(String productDocument) {
            this.productDocument = productDocument;
        }

        public String getProductGroup() {
            return productGroup;
        }

        public void setProductGroup(String productGroup) {
            this.productGroup = productGroup;
        }

        public String getSignature() {
            return signature;
        }

        public void setSignature(String signature) {
            this.signature = signature;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    private static class RequestSerializer extends StdSerializer<CreateDocumentRequest> {
        public RequestSerializer() {
            super(CreateDocumentRequest.class);
        }

        @Override
        public void serialize(CreateDocumentRequest createDocumentRequest, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("document_format", createDocumentRequest.getDocumentFormat().name());
            jsonGenerator.writeStringField("product_document", Base64.getEncoder().encodeToString(createDocumentRequest.getProductDocument().getBytes()));
            jsonGenerator.writeStringField("product_group", createDocumentRequest.getProductGroup());
            jsonGenerator.writeStringField("signature", Base64.getEncoder().encodeToString(createDocumentRequest.getSignature().getBytes()));
            jsonGenerator.writeStringField("type", createDocumentRequest.getType());
            jsonGenerator.writeEndObject();
        }
    }

    public String createDocument(Document document, String signature) throws JsonProcessingException {
        CreateDocumentRequest documentRequest = new CreateDocumentRequest(document, "milk", signature);
        URI uri = makeUri("/api/v3/lk/documents/create");
        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .headers("Content-Type", "application/json; charset=UTF-8", "Accept", "application/json")
                .header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(documentRequest)))
                .build();

        HttpResponse<String> response;
        while (!Thread.interrupted()) {
            if (limiter.isPossibleSendRequest()) {
                try {
                    response = client.send(request, HttpResponse.BodyHandlers.ofString());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                limiter.writeHistory();
                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    Map<String, String> jsonMap = getJsonMap(response);
                    return jsonMap.get("value");
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    private String getToken() {
        URI uri = makeUri("api/v3/auth/cert/key");
        Map<String, String> getMap = getTokenRequest(uri);
        // Signing data with УКЭП
        /*  some code */
        // And then
        String encodedData = encodeData(getMap);
        getMap.put("data", encodedData);
        Map<String, String> postMap = postTokenRequest(uri, getMap);
        String token = postMap.get("token");

        // returning stub at current moment
        return "0d90d966-5027-416f-a0cd-0697db8c79f3";
    }

    private String encodeData(Map<String, String> getMap) {
        try {
            return Base64.getEncoder().encodeToString(getMap.get("data").getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private URI makeUri(String path) {
        return URI.create(baseUrl + path);
    }

    private Map<String, String> postTokenRequest(URI uri, Map<String, String> body) {
        try {
            HttpRequest postReq = HttpRequest.newBuilder()
                    .uri(uri)
                    .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
                    .build();
            HttpResponse<String> response = client.send(postReq, HttpResponse.BodyHandlers.ofString());
            return getJsonMap(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> getTokenRequest(URI uri) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .GET()
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return getJsonMap(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, String> getJsonMap(HttpResponse<String> response) {
        int status = response.statusCode();
        if (status >= 200 && status < 300) {
            try {
                return mapper.readValue(response.body(), new TypeReference<>() {
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException();
        }
    }
}

@JsonSerialize(using = DocumentSerializer.class)
class Document {
    private Description description;
    private String docId;
    private String docStatus;
    private String docType;
    private boolean importRequest;
    private String ownerInn;
    private String participantInn;
    private String producerInn;
    private LocalDate productionDate;
    private ProductionType productionType;
    private Product[] products;
    private ZonedDateTime regDate;
    private String regNumber;

    enum ProductionType {
        OWN_PRODUCTION, CONTRACT_PRODUCTION;
    }

    static class Description {

        private String participantInn;

        public Description() {
        }

        public Description(String participantInn) {
            this.participantInn = participantInn;
        }

        public String getParticipantInn() {
            return participantInn;
        }

        public void setParticipantInn(String participantInn) {
            this.participantInn = participantInn;
        }
    }

    public Description getDescription() {
        return description;
    }

    public void setDescription(Description description) {
        this.description = description;
    }

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public String getDocStatus() {
        return docStatus;
    }

    public void setDocStatus(String docStatus) {
        this.docStatus = docStatus;
    }

    public String getDocType() {
        return docType;
    }

    public void setDocType(String docType) {
        this.docType = docType;
    }

    public boolean isImportRequest() {
        return importRequest;
    }

    public void setImportRequest(boolean importRequest) {
        this.importRequest = importRequest;
    }

    public String getOwnerInn() {
        return ownerInn;
    }

    public void setOwnerInn(String ownerInn) {
        this.ownerInn = ownerInn;
    }

    public String getParticipantInn() {
        return participantInn;
    }

    public void setParticipantInn(String participantInn) {
        this.participantInn = participantInn;
    }

    public String getProducerInn() {
        return producerInn;
    }

    public void setProducerInn(String producerInn) {
        this.producerInn = producerInn;
    }

    public LocalDate getProductionDate() {
        return productionDate;
    }

    public void setProductionDate(LocalDate productionDate) {
        this.productionDate = productionDate;
    }

    public ProductionType getProductionType() {
        return productionType;
    }

    public void setProductionType(ProductionType productionType) {
        this.productionType = productionType;
    }

    public Product[] getProducts() {
        return products;
    }

    public void setProducts(Product[] products) {
        this.products = products;
    }

    public ZonedDateTime getRegDate() {
        return regDate;
    }

    public void setRegDate(ZonedDateTime regDate) {
        this.regDate = regDate;
    }

    public String getRegNumber() {
        return regNumber;
    }

    public void setRegNumber(String regNumber) {
        this.regNumber = regNumber;
    }
}

class DocumentSerializer extends StdSerializer<Document> {
    private static final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")
            .withZone(ZoneId.of("Europe/Moscow"));

    protected DocumentSerializer() {
        super(Document.class);
    }

    @Override
    public void serialize(Document document, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeObjectField("description", document.getDescription());
        jsonGenerator.writeStringField("doc_id", document.getDocId());
        jsonGenerator.writeStringField("doc_status", document.getDocStatus());
        jsonGenerator.writeStringField("doc_type", document.getDocType());
        jsonGenerator.writeStringField("importRequest", String.valueOf(document.isImportRequest()));
        jsonGenerator.writeStringField("owner_inn", document.getOwnerInn());
        jsonGenerator.writeStringField("participant_inn", document.getParticipantInn());
        jsonGenerator.writeStringField("production_date", dateFormat.format(document.getProductionDate()));
        jsonGenerator.writeStringField("production_type", document.getProductionType().name());
        jsonGenerator.writeArrayFieldStart("products");
        if (document.getProducts().length > 0) {
            for (Product product : document.getProducts()) {
                jsonGenerator.writeStartObject();
                jsonGenerator.writeStringField("certificate_document", product.getCertificateDocument() == null ? null : product.getCertificateDocument().name());
                jsonGenerator.writeStringField("certificate_document_date", product.getCertificateDocumentDate() == null ? null : dateFormat.format(product.getCertificateDocumentDate()));
                jsonGenerator.writeStringField("certificate_document_number", product.getCertificateDocumentNumber());
                jsonGenerator.writeStringField("owner_inn", product.getOwnerInn());
                jsonGenerator.writeStringField("producer_inn", product.getProducerInn());
                jsonGenerator.writeStringField("production_date", product.getProductionDate() == null ? null : dateFormat.format(product.getProductionDate()));
                jsonGenerator.writeStringField("tnved_code", product.getTnvedCode());
                jsonGenerator.writeStringField("uit_code", product.getUitCode());
                jsonGenerator.writeStringField("uitu_code", product.getUituCode());
                jsonGenerator.writeEndObject();
            }
        }
        jsonGenerator.writeEndArray();
        jsonGenerator.writeStringField("reg_date", dateTimeFormat.format(document.getRegDate()));
        jsonGenerator.writeStringField("ref_number", document.getRegNumber());
        jsonGenerator.writeEndObject();
    }
}

class Product {
    private Certificate certificateDocument;
    private LocalDate certificateDocumentDate;
    private String certificateDocumentNumber;
    private String ownerInn;
    private String producerInn;
    private LocalDate productionDate;
    private String tnvedCode;
    private String uitCode;
    private String uituCode;

    public Product(Certificate certificateDocument, LocalDate certificateDocumentDate,
                   String certificateDocumentNumber, String ownerInn,
                   String producerInn, LocalDate productionDate, String tnvedCode, String uitCode, String uituCode) {
        this.certificateDocument = certificateDocument;
        this.certificateDocumentDate = certificateDocumentDate;
        this.certificateDocumentNumber = certificateDocumentNumber;
        this.ownerInn = ownerInn;
        this.producerInn = producerInn;
        this.productionDate = productionDate;
        this.tnvedCode = tnvedCode;
        this.uitCode = uitCode;
        this.uituCode = uituCode;
    }

    public Product() {
    }

    enum Certificate {
        CONFORMITY_CERTIFICATE, CONFORMITY_DECLARATION
    }

    public Certificate getCertificateDocument() {
        return certificateDocument;
    }

    public LocalDate getCertificateDocumentDate() {
        return certificateDocumentDate;
    }

    public String getCertificateDocumentNumber() {
        return certificateDocumentNumber;
    }

    public String getOwnerInn() {
        return ownerInn;
    }

    public String getProducerInn() {
        return producerInn;
    }

    public LocalDate getProductionDate() {
        return productionDate;
    }

    public String getTnvedCode() {
        return tnvedCode;
    }

    public String getUitCode() {
        return uitCode;
    }

    public String getUituCode() {
        return uituCode;
    }
}

/**
 * Ссылка на библиотеку https://github.com/axel-n/limiter-sliding-window.git
 */
class Limiter {
    private final int maxRequests;
    private final long sizeWindowInMilliseconds;
    private final Queue<Long> historyRequests = new ConcurrentLinkedQueue<>();

    private final AtomicReference<Integer> counterRequests = new AtomicReference<>(0);

    private final long periodForCheckExecutionInMilliseconds;
    private final long maxAwaitExecutionTimeInMilliseconds;

    public Limiter(int maxRequests, int sizeWindow,
                   long periodForCheckExecution, long maxAwaitExecution, TimeUnit interval) {
        this.maxRequests = maxRequests;
        this.sizeWindowInMilliseconds = interval.toMillis(sizeWindow);
        this.periodForCheckExecutionInMilliseconds = TimeUnit.MILLISECONDS.toMillis(periodForCheckExecution);
        this.maxAwaitExecutionTimeInMilliseconds = TimeUnit.SECONDS.toMillis(maxAwaitExecution);
        ExecutorService cleanHistoryExecutor = Executors.newSingleThreadExecutor();
        cleanHistoryExecutor.execute(this::cleanHistory);
    }

    public boolean isPossibleSendRequest() {
        int currentCounter = counterRequests.get();

        if (currentCounter > maxRequests) {
            return false;
        }

        while (!Thread.interrupted()) {
            int latestValue = counterRequests.get();

            if (latestValue >= maxRequests) {
                return false;
            }

            if (counterRequests.compareAndSet(latestValue, latestValue + 1)) {
                return true;
            }
        }
        return false;
    }

    public void writeHistory() {
        writeHistory(System.currentTimeMillis());
    }

    private void writeHistory(long timestamp) {
        historyRequests.add(timestamp);
    }

    private void cleanHistory() {
        while (!Thread.interrupted()) {
            if (!historyRequests.isEmpty()) {
                long now = System.currentTimeMillis();

                for (long current : historyRequests) {
                    if (isOld(now, current, sizeWindowInMilliseconds)) {
                        historyRequests.poll();
                        decrementFirstCounter();
                    } else {
                        break;
                    }
                }
            }
        }
    }

    private void decrementFirstCounter() {
        while (!Thread.interrupted()) {
            int currentValue = counterRequests.get();

            if (counterRequests.compareAndSet(currentValue, currentValue - 1)) {
                return;
            }
        }
    }

    private boolean isOld(long now, long timeRequest, long maxInterval) {
        return (now - timeRequest) > maxInterval;
    }
}