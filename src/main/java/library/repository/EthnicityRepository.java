package library.repository;

import library.entity.Ethnicity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EthnicityRepository extends JpaRepository<Ethnicity, Integer> {
}
