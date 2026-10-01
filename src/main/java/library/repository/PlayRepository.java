package library.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import library.entity.Play;

@Repository
public interface PlayRepository extends JpaRepository<Play, Integer> {
}
