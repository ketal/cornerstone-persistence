/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ketal.cornerstone.persistence.jpa.controller;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractJpaController<T> implements JpaController<T> {

    private static final Logger log = LogManager.getLogger(AbstractJpaController.class);

    private EntityManagerFactory emf = null;

    private Class<T> classType;

    protected AbstractJpaController(EntityManagerFactory emf, Class<T> classType) {
        this.emf = emf;
        this.classType = classType;
    }

    protected EntityManager getEntityManager() {
        return emf.createEntityManager();
    }
    
    protected abstract SingularAttribute<T, ?> getValidOrDefaultOrderBy(String orderBy);
    
    protected abstract List<Predicate> getSearchPredicates(T entity, CriteriaBuilder cb, Root<T> root);
    
    
    public List<T> getAll() {
        return get(true, -1, -1, null, false);
    }

    public List<T> get(int startPosition, int maxResults) {
        return get(false, startPosition, maxResults, null, false);
    }

    public List<T> get(int startPosition, int maxResults, String orderBy, boolean desc) {
        return get(false, startPosition, maxResults, orderBy, desc);
    }
    
    private List<T> get(boolean all, int startPosition, int maxResults, String orderBy, boolean desc) {
        EntityManager em = null;
        try {
            em = getEntityManager();
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<T> cq = cb.createQuery(this.classType);
            Root<T> root = cq.from(this.classType);
            cq.select(root);
            Path<?> orderByExpression = root.get(getValidOrDefaultOrderBy(orderBy));
            if(desc) {
                cq.orderBy(cb.desc(orderByExpression));
            } else {
                cq.orderBy(cb.asc(orderByExpression));
            }
            TypedQuery<T> q = em.createQuery(cq);
            if (!all) {
                q.setFirstResult(startPosition);
                q.setMaxResults(maxResults);
            }
            return q.getResultList();
        } finally {
            if (em != null /* && em.isOpen() */) {
                em.close();
            }
        }
    }
    
    public Long getCount() {
        EntityManager em = null;
        try {
            em = getEntityManager();
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<Long> cq = cb.createQuery(Long.class);
            Root<T> query = cq.from(this.classType);
            cq.select(cb.count(query));
            TypedQuery<Long> q = em.createQuery(cq);
            return q.getSingleResult();
        } finally {
            if (em != null /* && em.isOpen() */) {
                em.close();
            }
        }
    }
    
    public T getReference(Object id) {
        EntityManager em = null;
        try {
            em = getEntityManager();
            return em.getReference(this.classType, convertToPrimaryKeyType(id));
        } finally {
            if (em != null /* && em.isOpen() */) {
                em.close();
            }
        }
    }

    public T find(T entity) {
        Object id = getPrimaryKey(entity);
        return findByPrimaryKey(id);
    }

    public T findByPrimaryKey(Object id) {
        EntityManager em = null;
        try {
            em = getEntityManager();
            return em.find(this.classType, convertToPrimaryKeyType(id));
        } finally {
            if (em != null /* && em.isOpen() */) {
                em.close();
            }
        }
    }
    
    public List<T> findBy(SingularAttribute<T, ?> field, Object value) {
        return findBy(field, value, true, -1, -1, null, false);
    }

    public List<T> findBy(SingularAttribute<T, ?> field, Object value, int firstResult, int maxResults) {
        return findBy(field, value, false, firstResult, maxResults, null, false);
    }

    public List<T> findBy(SingularAttribute<T, ?> field, Object value, int firstResult, int maxResults, String orderBy, boolean desc) {
        return findBy(field, value, false, firstResult, maxResults, orderBy, desc);
    }
    
    private List<T> findBy(SingularAttribute<T, ?> field, Object value, boolean all, int firstResult, int maxResults, String orderBy, boolean desc) {
        EntityManager em = null;
        try {
            em = getEntityManager();
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<T> cq = cb.createQuery(this.classType);
            Root<T> root = cq.from(this.classType);
            cq.select(root);
            cq.where(cb.equal(root.get(field), value));
            Path<?> orderByExpression = root.get(getValidOrDefaultOrderBy(orderBy));
            if(desc) {
                cq.orderBy(cb.desc(orderByExpression));
            } else {
                cq.orderBy(cb.asc(orderByExpression));
            }
            TypedQuery<T> q = em.createQuery(cq);
            if (!all) {
                q.setFirstResult(firstResult);
                q.setMaxResults(maxResults);
            }
            return q.getResultList();
        } finally {
            if (em != null /* && em.isOpen() */) {
                em.close();
            }
        }
    }

    public Long findByCount(SingularAttribute<T, ?> field, Object value) {
        EntityManager em = null;
        try {
            em = getEntityManager();
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<Long> cq = cb.createQuery(Long.class);
            Root<T> query = cq.from(this.classType);
            cq.select(cb.count(query));
            cq.where(cb.equal(query.get(field), value));
            TypedQuery<Long> q = em.createQuery(cq);
            return q.getSingleResult();
        } finally {
            if (em != null /* && em.isOpen() */) {
                em.close();
            }
        }
    }
    
    public List<T> findBy(T searchEntity) {
        return findBy(searchEntity, true, -1, -1, null, false);
    }

    public List<T> findBy(T searchEntity, int firstResult, int maxResults) {
        return findBy(searchEntity, false, firstResult, maxResults, null, false);
    }

    public List<T> findBy(T searchEntity, int firstResult, int maxResults, String orderBy, boolean desc) {
        return findBy(searchEntity, false, firstResult, maxResults, orderBy, desc);
    }
    
    private List<T> findBy(T searchEntity, boolean all, int firstResult, int maxResults, String orderBy, boolean desc) {
        EntityManager em = null;
        try {
            em = getEntityManager();
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<T> cq = cb.createQuery(this.classType);
            Root<T> root = cq.from(this.classType);
            List<Predicate> predicates = getSearchPredicates(searchEntity, cb, root);
            cq.select(root);
            cq.where(predicates.toArray(new Predicate[predicates.size()]));
            Path<?> orderByExpression = root.get(getValidOrDefaultOrderBy(orderBy));
            if(desc) {
                cq.orderBy(cb.desc(orderByExpression));
            } else {
                cq.orderBy(cb.asc(orderByExpression));
            }
            TypedQuery<T> q = em.createQuery(cq);
            if (!all) {
                q.setFirstResult(firstResult);
                q.setMaxResults(maxResults);
            }
            return q.getResultList();
        } finally {
            if (em != null /* && em.isOpen() */) {
                em.close();
            }
        }
    }
    
    public Long findByCount(T searchEntity) {
        EntityManager em = null;
        try {
            em = getEntityManager();
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<Long> cq = cb.createQuery(Long.class);
            Root<T> root = cq.from(this.classType);
            List<Predicate> predicates = getSearchPredicates(searchEntity, cb, root);
            cq.select(cb.count(root));
            cq.where(predicates.toArray(new Predicate[predicates.size()]));
            TypedQuery<Long> q = em.createQuery(cq);
            return q.getSingleResult();
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
        return this.performTransaction(entity, (EntityManager em, T entity2) -> em.merge(entity2));
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
            if (em != null && em.getTransaction().isActive()) {
                log.debug("Rolling back transaction because of exception: {}", ex.getMessage());
                em.getTransaction().rollback();
            }
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
    
    protected String getLikeString(String queryValue) {
        StringBuilder builder = new StringBuilder();
        builder.append("%");
        builder.append(queryValue.toUpperCase());
        builder.append("%");
        return builder.toString();
    }

}
