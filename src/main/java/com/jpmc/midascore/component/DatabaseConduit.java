package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Component
public class DatabaseConduit {
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final RestTemplate restTemplate;

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseConduit.class);

    public DatabaseConduit(UserRepository userRepository, TransactionRepository transactionRepository, RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "transactions", groupId = "midas-core")
    @Transactional
    public void processTransaction(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        if (sender == null) {
            LOG.error("Sender with id {} not found", transaction.getSenderId());
            return;
        }

        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        if (recipient == null) {
            LOG.error("Recipient with id {} not found", transaction.getRecipientId());
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            LOG.error("Sender with id {} has insufficient funds", transaction.getSenderId());
            return;
        }

        float incentiveAmount = 0.0f;
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Transaction> requestEntity = new HttpEntity<>(transaction, headers);

            ResponseEntity<Incentive> response = restTemplate.exchange(
                    "http://localhost:8080/incentive",
                    HttpMethod.POST,
                    requestEntity,
                    Incentive.class
            );

            if (response.getBody() != null && response.getBody().getAmount() >= 0) {
                incentiveAmount = response.getBody().getAmount();
            }
        } catch (RestClientException e) {
            LOG.error("Error fetching incentive for transaction {}: {}", transaction, e.getMessage());
            // Proceed without incentive if API call fails
        }

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        transactionRepository.save(new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount));
        userRepository.save(sender);
        userRepository.save(recipient);

        LOG.info("Transaction processed: {}", transaction);
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }
}
