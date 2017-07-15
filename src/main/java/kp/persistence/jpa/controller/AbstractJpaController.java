package kp.persistence.jpa.controller;

import java.io.Serializable;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaQuery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractJpaController<T> implements JpaController<T>, Serializable {
    private static final long serialVersionUID = 1L;

    private final Logger logger = LogManager.getLogger(AbstractJpaController.class);

    private EntityManagerFactory emf = null;
    
    private Class<T> classType;

    protected AbstractJpaController(EntityManagerFactory emf, Class<T> classType) {
        this.emf = emf;
        this.classType = classType;
    }

    protected EntityManager getEntityManager() {
        return emf.createEntityManager();
    }

    public abstract Object getPrimaryKey(T entity);
    
    public List<T> getAll() {
        return get(true, -1, -1);
    }

    public List<T> get(int firstResult, int maxResults) {
        return get(false, firstResult, maxResults);
    }

    private List<T> get(boolean all, int firstResult, int maxResults) {
        EntityManager em = null;
        try {
            em = getEntityManager();
            CriteriaQuery<T> cq = em.getCriteriaBuilder().createQuery(this.classType);
            cq.select(cq.from(this.classType));
            TypedQuery<T> q = em.createQuery(cq);
            if (!all) {
                q.setMaxResults(maxResults);
                q.setFirstResult(firstResult);
            }
            return q.getResultList();
        } finally {
            if (em != null /* && em.isOpen() */) {
                em.close();
            }
        }
    }
    
    public T find(int id) {
        return findByPrimaryKey(id);
    }

    public T find(String id) {
        return findByPrimaryKey(id);
    }
    
    public T find(T entity) {
        Object id = getPrimaryKey(entity);
        return findByPrimaryKey(id);
    }

    private T findByPrimaryKey(Object id) {
        EntityManager em = null;
        try {
            em = getEntityManager();
            return em.find(this.classType, id);
        } finally {
            if (em != null /* && em.isOpen() */) {
                em.close();
            }
        }
    }
    
    public T create(T entity) {
        return this.performTransaction(entity, (EntityManager em, T entity2) -> em.persist(entity2));
    }

    public T update(T entity) {
        return this.performTransaction(entity, (EntityManager em, T entity2) -> em.merge(entity));
    }

    public T delete(T entity) {
        return this.performTransaction(entity, (EntityManager em, T entity2) -> {
            entity2 = em.find(this.classType, getPrimaryKey(entity2));
            if (entity2 == null) {
                return;
            }
            em.remove(entity2);
        });
    }

    private T performTransaction(T entity, Transaction<T> transaction) {
        EntityManager em = null;
        try {
            em = getEntityManager();
            em.getTransaction().begin();
            transaction.execute(em, entity);
            em.getTransaction().commit();
            return entity;
        } catch (Exception ex) {
            if (em.getTransaction().isActive()) {
                logger.debug("Rolling back transaction");
                em.getTransaction().rollback();
            }

            // logger.error("Caught Exeption: ", ex);
            throw ex;
        } finally {
            if (em != null /* && em.isOpen() */) {
                em.close();
            }
        }
    }

    private interface Transaction<T> {
        public void execute(EntityManager em, T entity);
    }
    
}
