package kg.khaadi.jooq.quickstart;


import kg.khaadi.jooq.quickstart.database.tables.records.*;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static kg.khaadi.jooq.quickstart.database.Tables.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.Records.mapping;
import static org.jooq.impl.DSL.multiset;

@Transactional
@SpringBootTest
class QueryTest {

    @Autowired
    private DSLContext dsl;

    @Test
    void find_all_films() {
        Result<FilmRecord> films = dsl.selectFrom(FILM).fetch();

        assertThat(films.size()).isEqualTo(1000);
    }

    @Test
    void find_all_actors_of_horror_films() {
        Result<Record2<String, String>> actorsOfHorrorFilms = dsl
                .select(ACTOR.FIRST_NAME, ACTOR.LAST_NAME)
                .from(ACTOR)
                .join(FILM_ACTOR).on(FILM_ACTOR.ACTOR_ID.eq(ACTOR.ACTOR_ID))
                .join(FILM).on(FILM_ACTOR.FILM_ID.eq(FILM.FILM_ID))
                .join(FILM_CATEGORY).on(FILM_CATEGORY.FILM_ID.eq(FILM.FILM_ID))
                .join(CATEGORY).on(FILM_CATEGORY.CATEGORY_ID.eq(CATEGORY.CATEGORY_ID))
                .where(CATEGORY.NAME.eq("Horror"))
                .groupBy(ACTOR.FIRST_NAME, ACTOR.LAST_NAME)
                .orderBy(ACTOR.FIRST_NAME, ACTOR.LAST_NAME)
                .fetch();


        assertThat(actorsOfHorrorFilms.size()).isEqualTo(155);
    }

    @Test
    void find_all_actors_of_horror_films_implicit_join() {
        Result<Record2<String, String>> actorsOfHorrorFilms = dsl
                .select(FILM_ACTOR.actor().FIRST_NAME, FILM_ACTOR.actor().LAST_NAME)
                .from(FILM_ACTOR)
                .join(FILM_CATEGORY).on(FILM_ACTOR.FILM_ID.eq(FILM_CATEGORY.FILM_ID))
                .where(FILM_CATEGORY.category().NAME.eq("Horror"))
                .groupBy(FILM_ACTOR.actor().FIRST_NAME, FILM_ACTOR.actor().LAST_NAME)
                .orderBy(FILM_ACTOR.actor().FIRST_NAME, FILM_ACTOR.actor().LAST_NAME)
                .fetch();

        assertThat(actorsOfHorrorFilms.size()).isEqualTo(155);
    }

    @Test
    void find_all_actors_of_horror_films_implicit_join_into_record() {
        List<ActorWithFirstAndLastName> actorsOfHorrorFilms = dsl
                .select(FILM_ACTOR.actor().FIRST_NAME, FILM_ACTOR.actor().LAST_NAME)
                .from(FILM_ACTOR)
                .join(FILM_CATEGORY).on(FILM_ACTOR.FILM_ID.eq(FILM_CATEGORY.FILM_ID))
                .where(FILM_CATEGORY.category().NAME.eq("Horror"))
                .groupBy(FILM_ACTOR.actor().FIRST_NAME, FILM_ACTOR.actor().LAST_NAME)
                .orderBy(FILM_ACTOR.actor().FIRST_NAME, FILM_ACTOR.actor().LAST_NAME)
                .fetchInto(ActorWithFirstAndLastName.class);

        assertThat(actorsOfHorrorFilms.size()).isEqualTo(155);
    }

    @Test
    void insert_film() {
        int insertedRows = dsl.
                insertInto(FILM)
                .columns(FILM.TITLE, FILM.LANGUAGE_ID)
                .values("Test", 1)
                .execute();

        assertThat(insertedRows).isEqualTo(1);
    }

    @Test
    void insert_film_with_set() {
        int insertedRows = dsl.
                insertInto(FILM)
                .set(FILM.TITLE, "Test set")
                .set(FILM.LANGUAGE_ID, 1)
                .execute();

        assertThat(insertedRows).isEqualTo(1);
    }

    @Test
    void insert_film_using_record() {
        FilmRecord filmRecord = dsl.newRecord(FILM);
        filmRecord.setTitle("Test");
        filmRecord.setLanguageId(1);
        int insertedRows = filmRecord.store();

        assertThat(insertedRows).isEqualTo(1);
    }

    @Test
    void find_film() {
        FilmRecord filmRecord = dsl
                .selectFrom(FILM)
                .where(FILM.FILM_ID.eq(1))
                .fetchOne();

        assertThat(filmRecord).isNotNull();
        assertThat(filmRecord.getTitle()).isEqualTo("ACADEMY DINOSAUR");
    }

    @Test
    void find_all_actors_with_films() {
        List<ActorWithFilms> actorWithFilms = dsl
                .select(ACTOR.FIRST_NAME,
                        ACTOR.LAST_NAME,
                        multiset(dsl.select(FILM_ACTOR.film().TITLE)
                                .from(FILM_ACTOR)
                                .where(FILM_ACTOR.ACTOR_ID.eq(ACTOR.ACTOR_ID)))
                                .convertFrom(r -> r.map(mapping(ActorWithFilms.FilmName::new)))
                )
                .from(ACTOR)
                .fetch(mapping(ActorWithFilms::new));

        assertThat(actorWithFilms.size()).isEqualTo(200);
    }

    @Test
    @Commit
    void add_actor() {

        ActorRecord actorRecord = dsl
                .insertInto(ACTOR, ACTOR.FIRST_NAME, ACTOR.LAST_NAME)
                .values("HAADI", "BOLOTBEKOV")
                .returning()
                .fetchOne();

        System.out.println("Inserted Actor: " + actorRecord);
    }

    @Test
    void add_actor_with_transaction() {
        dsl.transaction(configuration -> {
            DSLContext ctx = DSL.using(configuration);

            ctx.insertInto(ACTOR)
                    .set(ACTOR.FIRST_NAME, "HAADI")
                    .set(ACTOR.LAST_NAME, "BOLOTBEKOV")
                    .execute();

            // Force rollback by throwing an exception
            // throw new RuntimeException("Just testing rollback");
        });

    }

    @Test
    @Commit
    void delete_actor() {

        boolean isDeleted = dsl
                .delete(ACTOR)
                .where(ACTOR.ACTOR_ID.eq(201))
                .execute() > 0;

        System.out.println("Deleted Actor: " + isDeleted);
    }


}
