package ru.practicum.user;

import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class UserRepositoryQDSL {
    private final JPAQueryFactory jpaQueryFactory;
    QUser userEntity = QUser.user;

    public User findById(long userId) {
        return jpaQueryFactory.selectFrom(userEntity).where(userEntity.id.eq(userId)).fetchOne();
    }
}
