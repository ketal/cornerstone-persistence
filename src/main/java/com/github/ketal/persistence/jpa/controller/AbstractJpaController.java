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
package com.github.ketal.persistence.jpa.controller;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractJpaController<T> implements JpaController<T> {

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

    public abstract SingularAttribute<T, ?> getDefaultOrderBy();

    public List<T> getAll() {
        return get(true, -1, -1);
    }

    public List<T> get(int startPosition, int maxResults) {
        return get(false, startPosition, maxResults);
    }

    private List<T> get(boolean all, int startPosition, int maxResults) {
        EntityManager em = null;
        try {
            em = getEntityManager();
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<T> cq = cb.createQuery(this.classType);
            Root<T> query = cq.from(this.classType);
            cq.select(query);
            cq.orderBy(cb.asc(query.get(getDefaultOrderBy())));
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

    public List<T> findBy(SingularAttribute<T, ?> field, Object value) {
        return findBy(field, value, true, -1, -1);
    }

    public List<T> findBy(SingularAttribute<T, ?> field, Object value, int firstResult, int maxResults) {
        return findBy(field, value, false, firstResult, maxResults);
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
            cq.orderBy(cb.asc(query.get(getDefaultOrderBy())));
            TypedQuery<Long> q = em.createQuery(cq);
            return q.getSingleResult();
        } finally {
            if (em != null /* && em.isOpen() */) {
                em.close();
            }
        }
    }

    private List<T> findBy(SingularAttribute<T, ?> field, Object value, boolean all, int firstResult, int maxResults) {
        EntityManager em = null;
        try {
            em = getEntityManager();
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<T> cq = cb.createQuery(this.classType);
            Root<T> query = cq.from(this.classType);
            cq.select(query);
            cq.where(cb.equal(query.get(field), value));
            cq.orderBy(cb.asc(query.get(getDefaultOrderBy())));
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
