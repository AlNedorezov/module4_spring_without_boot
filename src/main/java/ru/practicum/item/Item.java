package ru.practicum.item;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Table;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = Item.TABLE_NAME)
@Getter
@Setter
@ToString
class Item {
    public static final String TABLE_NAME = "items";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id")
    private Long userId;

    @Column
    private String url;
    // здесь остальные поля

    @ElementCollection
    @CollectionTable(name = "tags", joinColumns = @JoinColumn(name = "item_id"))
    @Column(name = "name")
    private Set<String> tags = new HashSet<>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Item)) return false;
        return id != null && id.equals(((Item) o).getId());
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}