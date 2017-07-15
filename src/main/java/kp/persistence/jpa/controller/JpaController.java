package kp.persistence.jpa.controller;

import java.util.List;

public interface JpaController<T> {

    abstract Object getPrimaryKey(T entity);

    public List<T> getAll();
    
    public List<T> get(int maxResults, int firstResult);
    
    public T find(int id);
    
    public T find(String id);
    
    public T find(T entity);

    public T create(T entity);
    
    public T update(T entity);

    public T delete(T entity);

}
