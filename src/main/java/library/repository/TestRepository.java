package library.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import library.entity.Persona;

@Repository
public interface TestRepository extends JpaRepository<Persona, Long>{

}
