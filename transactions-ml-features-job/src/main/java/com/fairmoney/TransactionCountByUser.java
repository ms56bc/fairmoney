package com.fairmoney;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

@Table(keyspace = "my_keyspace", name = "transaction_count")
public class TransactionCountByUser implements Serializable {
    private static final long serialVersionUID = 1123119384361005680L;

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Column(name = "user_id")
    private int userId;
    @Column(name = "count")
    private Long count;

    @Column(name = "created_at")
    private Long createdAt;

    public TransactionCountByUser() {
        this(0, null, null);
    }

    public TransactionCountByUser(int userId, Long count, Long createdAt) {
        this.userId = userId;
        this.count = count;
        this.createdAt = createdAt;
    }

    public int getUserId() {
        return userId;
    }

    public Long getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "TransactionCountByUser{" +
                "userId=" + userId +
                ", count=" + count +
                ", createdAt=" + createdAt +
                '}';
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    public UserStats asUserStatsAvro() {
        return UserStats.newBuilder()
                .setUserId(userId)
                .setTotalTransactionsCount(count)
                .build();
    }
}
