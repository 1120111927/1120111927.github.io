---
title: Java集合框架
date: "2021-03-31"
description: Java中的集合框架定义了一套规范，用来表示、操作集合，使具体操作与实现细节解耦
tags: collection, List, Set, Map
---

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members
interface Collection
abstract class AbstractCollection
Collection <|.. AbstractCollection

interface List
abstract class AbstractList
class Vector
class Stack
class ArrayList
abstract class AbstractSequentialList
class LinkedList
Collection <|.. List
List <|.. AbstractList
AbstractCollection <|.. AbstractList
AbstractList <|-- Vector
AbstractList <|-- ArrayList
AbstractList <|.. AbstractSequentialList
Vector <|-- Stack
AbstractSequentialList <|-- LinkedList
Deque <|-- LinkedList

interface Set
interface SortedSet
abstract class AbstractSet
class HashSet
class LinkedHashSet
class TreeSet
Collection <|.. Set
Set <|.. AbstractSet
AbstractCollection <|.. AbstractSet
AbstractSet <|-- HashSet
HashSet <|-- LinkedHashSet
AbstractSet <|-- TreeSet
Set <|-- SortedSet
SortedSet <|-- TreeSet

interface Queue
interface Deque
abstract class AbstractQueue
Collection <|.. Queue
Queue <|.. AbstractQueue
AbstractCollection <|.. AbstractQueue
AbstractQueue <|-- PriorityQueue
Queue <|.. Deque
AbstractCollection <|.. ArrayDeque
Deque <|.. ArrayDeque

interface Map
interface SortedMap
interface NavigableMap
abstract class AbstractMap
class Dictionary
class HashTable
class Properties
class EnumMap
class HashMap
class LinkedHashMap
class TreeMap

Map <|.. HashTable
Map <|.. EnumMap
Map <|.. HashMap
Map <|.. AbstractMap
Map <|.. SortedMap
AbstractMap <|-- EnumMap
AbstractMap <|-- HashMap
AbstractMap <|-- TreeMap
Dictionary <|-- HashTable
HashTable <|-- Properties
HashMap <|-- LinkedHashMap
SortedMap <|.. NavigableMap
NavigableMap <|.. TreeMap

@enduml
```

数据结构是以某种形式将数据组织在一起的集合，不仅存储数据，还支持访问和处理数据的操作。Java中提供的组织和操作数据的数据结构统称为Java集合规范。

集合代表了一组对象，Java中的集合框架定义了一套规范，用来表示、操作集合，使具体操作与实现细节解耦。

Java集合框架包含如下内容：

+ 接口：代表集合的抽象数据类型，Collection、List、Set、Map
+ 实现：集合接口的具体实现，如ArrayList、LinkedList、HashSet、HashMap
+ 算法：实现集合接口的对象里包含的方法执行的一些计算

所有集合类都位于java.util包下，但支持多线程的集合类位于java.util.concurrent包下。

Java集合框架的类继承体系中，Collection和Map是最顶层的两个接口，Collection表示单值容器，Map表示关联容器。

+ Set：集合，不允许有重复元素的容器
+ List：列表，允许有重复元素的有序容器
+ Map：映射（或关联数组、字典），键值对容器，可根据元素的键访问值

Map接口提供了三种集合视角（collection views），使得可以像集合一样操作它们：key的集合、value的集合、key-value映射的集合。

Java集合框架采用适配器模式，创建实现接口的抽象类，在抽象类中实现接口中的若干或全部方法，具体的集合实现类只需直接继承抽象类，并实现自己需要的方法即可，而不用实现接口中的全部抽象方法。AbstractCollection、AbstractSet、AbstractList、AbstractSequentialList、AbstractMap提供了核心集合接口的基本实现[^骨架实现（SI，skeletal implementation）：集接口多继承与抽象类减少重复代码的优势于一体]。

集合实现类一般遵循<实现方式>+<接口>的命名方式：

|Interface|Hash Table|Resizable Array|Balance Tree|Linked List|Hash Table + Linked List|
|---|---|---|---|---|---|
|**Set**|HashSet||TreeSet||LinkedHashSet|
|**List**||ArrayList||LinkedList||
|**Deque**||ArrayDeque||LinkedList||
|**Map**|HashMap||TreeMap||LinkedHashMap|

常用的集合实现类有：ArrayList、LinkedList、ArrayDeque、HashSet、HashMap、TreeMap。

HashTable、Vector、Stack、Enumeration\<E\>是遗留类/接口，不推荐继续使用。

数组与集合的区别：

+ 数组长度不可变化且无法保存具有映射关系的数据；集合类用于保存数量不确定的数据，以及保存具有映射关系的数据
+ 数组元素既可以是基本类型的值，也可以是对象；集合只能保存对象

## Collection

`interface Collection<E> extends Iterable<E>`

查询操作：

+ `int size()`：返回集合中元素的数目，如果元素数目大于Integer.MAX_VALUE，返回Integer.MAX_VALUE
+ `boolean isEmpty()`：判断集合是否为空
+ `boolean contains(Object o)`：判断集合是否包含指定元素
+ `Object[] toArray()`：将集合转为数组
+ `<T> T[] toArray(T[] a)`：将集合转为数组
+ `Iterator<E> iterator()`：返回集合中元素的iterator
+ `Spliterato<E> spliterator()`：返回集合中元素的spliterator
+ `Stream<E> stream()`：
+ `Stream<E> parallelStream()`：

修改操作：

+ `boolean add(E e)`：添加元素
+ `boolean remove(Object o)`：移除元素

批量操作：

+ `boolean containsAll(Collection<?> c)`：判断集合是否包含指定集合中的所有元素
+ `boolean addAll(Collection<? extends E> c)`：向集合中添加指定集合中的所有元素
+ `boolean removeAll(Collection<?> c)`：从集合中移除指定集合中的元素
+ `boolean retainAll(Collection<?> c)`：仅保留指定集合中包含的元素
+ `void clear()`：清空集合
+ `boolean removeIf(Predicate<? super E> filter)`：从集合中移除所有满足条件的元素

AbstractCollection是Collection接口的骨架实现。

`abstract class AbstractCollection<E> implements Collection<E>`

## List

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members
interface Collection
abstract class AbstractCollection
interface List
abstract class AbstractList
abstract class AbstractSequentialList
class LinkedList
class ArrayList

Collection <|.. AbstractCollection
Collection <|-- List
List <|.. AbstractList
AbstractCollection <|-- AbstractList
AbstractList <|-- AbstractSequentialList
AbstractList <|-- ArrayList
AbstractSequentialList <|-- LinkedList
@enduml
```

List是有序集合，元素被添加到容器中的特定位置，可以通过iterator或索引（从0开始）读写元素

`interface List<E> extends Collection<E>`

批量修改操作：

+ `boolean addAll(int index, Collection<? extends E> c)`：添加指定集合中的所有元素
+ `void replaceAll(UnaryOperator<E> operator)`：使用对元素执行操作后的结果替换该元素
+ `void sort(Comparator<? super E> c)`：按照指定的Comparator对列表排序

位置访问操作：

+ `E get(int index)`：获取指定位置的元素
+ `E set(int index, E element)`：使用指定元素替换指定位置的元素
+ `void add(int index, E element)`：向指定位置插入指定元素，同时将该位置及其右侧元素右移
+ `E remove(int index)`：移除指定位置的元素

查找操作：

+ `int indexOf(Object o)`：返回指定元素在列表中第一次出现的位置，如果元素不存在则返回-1
+ `int lastIndex(Object o)`：返回指定元素在列表中最后一次出现的位置，如果元素不存在则返回-1

ListIterator：

+ `ListIterator<E> listIterator()`：返回list iterator
+ `ListIterator<E> listIterator(int index)`：返回列表中指定位置及之后元素的list iterator

视图：

+ `List<E> subList(int fromIndex, int toIndex)`：返回[fromIndex, toIndex)区域的视图，如果fromIndex和toIndex相等，则返回空列表

AbstractList是List接口的骨架实现。

`abstract class AbstractList<E> extends AbstractCollection<E> implements List<E>`。

AbstractSequentialList是基于顺序访问数据存储实现List的骨架实现，基于列表迭代器实现随机访问方法（`get(int index)`、`set(int index, E element)`、`add(int index, E element)`、`remove(int index)`）。

`class AbstractSequentialList<E> extends AbstractList<E>`

### ArrayList

ArrayList是基于动态数组（resizable-array）实现的列表，允许插入任何符合规则的元素（包括null）。每次向容器中添加元素时，都会检查容量，快溢出时就会进行扩容操作。几乎相当于Vector，除了它不是同步的。

`size()`、`isEmpty()`、`get()`、`set()`、`iterator()`、`listIterator()`操作均为常数时间，`add()`操作具有均摊常数时间，所有其他操作均为线性时间。

`class ArrayList<E> extends AbstractList<E> implements List<E>, RandomAccess, Cloneable, java.io.Serializable`

构造器：

+ `ArrayList()`：构造默认容量（DEFAULT_CAPACITY = 10）的ArrayList
+ `ArrayList(int initialCapacity)`：构造指定容量的ArrayList
+ `ArrayList(Collection<? extends E> c)`：构造包含指定集合中所有元素的ArrayList，顺序为指定集合迭代器遍历元素的顺序

+ `void trimToSize()`：减小ArrayList实例的容量到列表当前大小，用于最小化ArrayList实例占用空间
+ `void ensureCapacity(int minCapacity)`：增加ArrayList实例的容量

## Set

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members

interface Collection
abstract class AbstractCollection
interface Set
abstract class AbstractSet
interface SortedSet
interface NavigableSet
class TreeSet
class HashSet
class LinkedHashSet

Collection <|.. AbstractCollection
Collection <|-- Set
Set <|-- SortedSet
Set <|.. AbstractSet
AbstractCollection <|-- AbstractSet
SortedSet <|-- NavigableSet
NavigableSet <|.. TreeSet
AbstractSet <|-- TreeSet
AbstractSet <|-- HashSet
HashSet <|-- LinkedHashSet
@enduml
```

Set是不包含重复元素的集合。

`interface Set<E> extends Collection<E>`

Abstract是Set接口的骨架实现。

SortedSet是支持排序的Set，元素按照自然顺序或指定的Comparator排序，iterator按照递增顺序遍历集合中的元素。

`interface SortedSet<E> extends Set<E>`

+ `Comparator<? super E> comparator()`：返回SortedSet使用的Comparator，按照元素自然顺序排序时返回null
+ `SortedSet<E> subSet(E fromElement, E toElement)`：返回集合中指定范围[fromElement, toElement)内的元素的视图
+ `SortedSet<E> headSet(E toElement)`：返回集合中小于指定元素的元素的视图
+ `SortedSet<E> tailSet(E fromElement)`：返回集合中大于等于指定元素的元素的视图
+ `E first()`：返回集合中的最小元素
+ `E last()`：返回集合中的最大元素

NavigableSet为SortedSet扩展了navigation方法来返回最靠近指定搜索目标的元素。方法`lower()`、`floor()`、`ceiling()`、`highter()`分别返回小于、小于等于、大于等于、大于指定元素的元素，没有这样的元素时返回null

`interface NavigableSet<E> extends SortedSet<E>`

+ `E lower(E e)`：返回集合中小于指定元素的最大元素，没有这样的元素时返回null
+ `E floor(E e)`：返回集合中小于等于指定元素的最大元素，没有这样的元素时返回null
+ `E ceiling(E e)`：返回集合中大于等于指定元素的最小元素，没有这样的元素时返回null
+ `E higher(E e)`：返回集合中大于指定元素的最小元素，没有这样的元素时返回null
+ `E pollFirst()`：返回并移除集合中的最小元素，集合为空时返回null
+ `E pollLast()`：返回并移除集合中的最大元素，集合为空时返回null
+ `NavigableSet<E> descendingSet()`：返回逆序遍历集合元素的视图
+ `Iterator<E> descendingIterator()`：返回一个逆序访问元素的迭代器
+ `NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive)`：返回集合中指定范围内元素的视图
+ `NavigableSet<E> headSet(E toElement, boolean inclusive)`：返回集合中小于（或等于，当inclusive为true时）指定元素的视图
+ `NavigableSet<E> tailSet(E fromElement, boolean inclusive)`：返回集合中大于（或等于，当inclusive为true时）指定元素的元素的视图

### HashSet

HashSet基于散列表（HashMap）实现了Set接口，支持null元素。散列函数均匀划分数据时，`add()`、`remove()`、`contain()`、`size()`等基本操作均为常数时间。

`class HashSet<E> extends AbstractSet<E> implements Set<E>, Cloneable, java.io.Serializable`

构造器：

+ `HashSet()`：构造一个空的HashSet，内部的HashMap实例为默认初始容量（16）与加载因子（0.75）
+ `HashSet(Collection<? extends E> c)`：构造一个包含指定集合中所有元素的HashSet
+ `HashSet(int initialCapacity)`：构造一个内部HashMap为指定容量的HashSet
+ `HashSet(int initialCapacity, float loadFactor)`：构造一个内部HashMap为指定容量和加载因子的HashSet

### TreeSet

TreeSet基于TreeMap实现了NavigableSet，元素必须实现Comparable接口，元素按照自然顺序或指定的Comparator排序，`add()`、`remove()`、`contains()`等基本操作均为对数时间（log(n)）

`class TreeSet<E> extends AbstractSet<E> implements NavigableSet<E>, Cloneable, java.io.Serializable`

构造器：

+ `TreeSet()`：构造一个空的TreeSet，根据元素自然顺序对元素排序
+ `TreeSet(Comparator<? super E> comparator)`：构造一个空的TreeSet，根据指定Comparator对元素排序
+ `TreeSet(Collection<? extends E> c)`：构造一个包含指定集合所有元素的TreeSet，根据元素自然顺序对元素排序
+ `TreeSet(SortedSet<E> s)`：构造一个包含指定SortedSet中所有元素的TreeSet，这个TreeSet按照相同的顺序对元素排序

### LinkedHashSet

## Map

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members
interface Map
interface SortedMap
interface NavigableMap
abstract class AbstractMap
class Dictionary
class HashTable
class Properties
class EnumMap
class HashMap
class LinkedHashMap
class TreeMap

Map <|.. HashTable
Map <|.. EnumMap
Map <|.. HashMap
Map <|.. AbstractMap
Map <|-- SortedMap
AbstractMap <|-- EnumMap
AbstractMap <|-- HashMap
AbstractMap <|-- TreeMap
Dictionary <|-- HashTable
HashTable <|-- Properties
HashMap <|-- LinkedHashMap
SortedMap <|-- NavigableMap
NavigableMap <|.. TreeMap

@enduml
```

Map用于存储键值对，不能包含重复的键，一个键只能对应一个值，Map接口提供了三种集合视图（collection views）：键的集合、值的集合、键值对的集合。

`interface Map<K, V>`

查询操作：

+ `int size()`：返回map中包含的键值对数目，大于Integer.MAX_VALUE时返回Integer.MAX_VALUE
+ `boolean isEmpty()`：检查map中是否包含键值对，为空时返回true
+ `boolean containsKey(Object key)`：检查map中是否包含指定键
+ `boolean containsValue(Object value)`：检查map中是否包含指定值
+ `V get(Object key)`：返回指定键对应的值

修改操作：

+ `V put(K key, V value)`：存入指定键值对，如果键已存在则覆盖其值
+ `V remove(Object key)`：移除包含指定键的键值对，并返回该键对应的值

批量操作：

+ `void putAll(Map<? extends K, ? extends V> m)`：添加指定map中的所有元素
+ `void clear()`：清空map

视图：

+ `Set<K> keySet()`：返回map中所有键的Set视图
+ `Collection<V> value()`：返回map中所有值的集合视图
+ `Set<Map.Entry<K, V>> entrySet()`：返回map中所有键值对的Set视图

默认方法：

+ `V getOrDefault(Object key, V defaultValue)`：返回map中指定键对应的值，键不存在时返回指定的值
+ `void forEach(BiConsumer<? super K, ? super V> action)`：对map中的每个键值对都执行指定的操作
+ `void replaceAll(BiFunction<? super K, ? extends V> function)`：对map中的每个键值对执行指定操作并用返回的值替换原值
+ `V putIfAbsent(K key, V value)`：添加指定键值对，键已经存在时返回当前值
+ `boolean remove(Object key, Object value)`：移除指定的键值对
+ `boolean replace(K key, V oldValue, V newValue)`：仅当键对应的值为指定值时更新为新值
+ `V replace(K key, V value)`：更新指定键对应的值为指定的值
+ `V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction)`：指定的键不存在时，通过指定的函数计算其对应的值并将键值对存入map
+ `V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction)`：指定的键存在且非null时，通过指定的函数计算其对应的值并更新现在对应的值，函数返回null时移除键值对
+ `V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction)`：
+ `V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction)`：

AbstractMap是Map接口的骨架实现。

`class AbstractMap<K, V> implements Map<K, V>`

### HashMap

HashMap基于散列表实现了Map接口，支持null键和null值。散列函数均匀分配元素时，`get()`、`put()`为常熟时间。有两个元素影响HashMap实例的性能：初始容量（initial capacity）和加载因子（load factor），容量是指散列表中桶（bucket）的数目，加载因子决定自动扩容时机。当散列表中的元素大于HashMap当前容量和加载因子的乘积时，散列表将发生rehash并扩容为原来的两倍。默认加载因为0.75较好的平衡了时间和空间，较大的值较低空间浪费，但是会增加查找时间。

JDK 1.8中HashMap实现可以理解为数组+链表+红黑树。

`class HashMap<K, V> extends AbstractMap<K, V> implements Map<K, V>, Cloneable, Serializable`

构造器：

+ `HashMap()`：构造一个默认初始容量（16）、默认加载因子（0.75）的HashMap
+ `HashMap(int initialCapacity)`：构造一个指定初始容量、默认加载因子的HashMap。推荐开发过程中评估好HashMap的大小，尽可能保证扩容的阈值大于存储元素的数量，减少其扩容次数
+ `HashMap(int initialCapacity, float loadFactor)`：构造一个指定初始容量和加载因子的HashMap
+ `HashMap(Map<? extends K, ? extends V> m)`：创建一个包含指定map中所有键值对的默认加载因子、初始容量足够容纳指定map中所有键值对的HashMap

#### 自动扩容

HashMap采用最小可用原则，容量超过一定阈值便自动进行扩容。扩容是通过`resize()`方法来实现的，在`putVal()`方法最后被调用，即写入元素之后才判断是否需要扩容操作，当增加元素后的size大于之前所计算好的阈值threshold，即执行resize操作。

```Java
if (++size > threshold)
    resize();
```

通过位运算`<<1`进行容量扩充，即扩容1倍，同时新的阈值newThr也扩容为老阈值的1倍。

```Java
final Node<K, V>[] resize() {
    // ...
    else if ((newCap = oldCap << 1) < MAXIMUM_CAPACITY && oldCap >= DEFAULT_INITIAL_CAPACITY)
        newThr = oldThr << 1;
    // ...
}
```

扩容时，总共存在三种情况：

+ 哈希桶数组中某个位置只有1个元素，即不存在哈希冲突时，直接将该元素复制到新哈希桶数组的对应位置即可
+ 哈希桶数组中某个位置的节点为树节点时，执行红黑树的扩容操作
+ 哈希桶数组中某个位置的节点为普通节点时，执行链表扩容操作，在JDK 1.8中，为了避免之前版本中并发扩容所导致的死链问题[^在JDK 1.8之前，HashMap在并发场景下扩容时存在一个bug，形成死链，导致get该位置元素的时候，会死循环，使CPU利用率居高不下]，引入了高低位链表辅助进行扩容操作

```Java
if (oldTab != null) {
    // 原数组不为null，数据迁移
    for (int j = 0; j < oldCap; ++j) {
        Node<K, V> e;
        if ((e = oldTab[j]) != null) {
            oldTab[j] = null;
            if (e.next == null)
                // 链表只有一个元素，直接复制
                newTab[e.hash & (newCap - 1)] = e;
            else if (e instanceof TreeNode)
                // 如果是Tree结构，执行红黑树扩容
                ((TreeNode<K, V>)e).split(this, newTab, j, oldCap);
            else { // preserve order
                // 链表数据迁移
                // [死链解决]采用高低位链表可以解决JDK 8之前，链表扩容导致的死链问题，先遍历完链表，最后再放入到哈希桶中对应的位置；而之前是每遍历链表中的一个item就放入到哈希桶中
                // 低位链表
                Node<K, V> loHead = null, loTail = null;
                // 高位链表
                Node<K, V> hiHead = null, hiTail = null;
                // 下一节点引用
                Node<K, V> next;
                do {
                    next = e.next;
                    // 为0说明扩容后再求余结果不变，为1扩容后再求余就是原索引+旧数据长度（j+oldCap）
                    // 0: 原位置，对应的是低位链表
                    // 1: 原位置索引 + 原数组长度，对应的是高位链表
                    if ((e.hash & oldCap) == 0) {
                        // TODO 重新创建低位链表
                        if (loTail == null)
                            loHead = e;
                        else
                            loTail.next = null;
                        loTail = e;
                    }
                    else {
                        // TODO 重新创建高位链表
                        if (hiTail == null)
                            hiHead = e;
                        else
                            hiTail.next = e;
                        hiTail = e;
                    }
                }
            }
        }
    }
}
```

**高低位链表**：在扩容时，哈希桶数组buckets会扩容一倍，以容量为8的HashMap为例，原有容量8扩容至16，将[0, 7]称为低位，[8, 15]称为高位，低位对应loHead、loTail，高位对应hiHead、hiTail。

### 初始化与懒加载

初始化时只会设置默认的负载因子，并不会进行其他初始化的操作，在首次使用的时候才会进行初始化。当创建一个新的HashMap的时候，不会立即对哈希数组进行初始化，而是在首次`put`元素的时候，通过`resize()`方法进行初始化。

```Java
final V putVal(int hash, K key, V value, boolean onlyIfAbsent, boolean evict) {
    // 声明变量，使用局部变量名代替直接使用全局变量名
    // tab：哈希桶
    // p：输入元素对应的哈希桶中节点
    // n：哈希桶大小
    // i：输入元素对应的哈希桶索引
    Node<K, V>[] tab;
    Node<K, V> p;
    int n, i;
    // 如果底层数组table没有初始化，则通过resize方法初始化数组
    if ((tab = table) == null || (n = tab.length) == 0)
        // 说明Map不是在new的时候初始化，而是在第一次put的时候初始化
        n = (tab = resize()).length;
    if ((p = tab[i = (n - 1) & hash]) == null)
        // 未出现哈希冲突
        tab[i] = newNode(hash, key, value, null);
    else {
        // 出现哈希冲突
        Node<K, V> e;
        K k;
        // 当前哈希桶位置中的节点与输入元素的键值一致，则直接替换
        if (p.hash == hash && ((k = p.key) == key || (key != null && key.equals(k))))
            e = p;
        // 当前哈希桶位置中的节点与输入元素的键值不一致，遍历该位置上的红黑树节点，选择合适位置插入
        else if (p instanceof TreeNode)
            e = ((TreeNode<K, V>)p).putTreeVal(this, tab, hash, key, value);
        // 当前哈希桶位置中的节点与输入元素的键值不一致，遍历该位置上的链表节点，选择合适位置插入
        // ...
    }
}
```

`resize()`中会设置默认的初始容量`DEFAULT_INITIAL_CAPACITY`为16，扩容的阈值为$0.75x16=12$，即哈希桶数组中元素达到12个便进行扩容操作。

最后创建容量为16的Node数组，并赋值给成员变量哈希桶table，即完成了HashMap的初始化操作。

```Java
final Node<K, V>[] resize() {
    // 使用局部变量持有table
    Node<K, V>[] oldTab = table;
    int oldCap = (oldTab == null) ? 0 : oldTab.length;
    int oldThr = threshold;
    // 新容量、阈值
    int newCap, newThr = 0;
    if (oldCap > 0) {
        // 如果原容量已经大于等于最大容量，则不再扩容
        if (oldCap >= MAXIMUM_CAPACITY) {
            threshold = Integer.MAX_VALUE;
            return oldTab;
        }
        // 否则，翻倍扩容
        else if ((newCap = oldCap << 1) < MAXIMUM_CAPACITY && oldCap >= DEFAULT_INITIAL_CAPACITY)
            newThr = oldThr << 1; // 阈值*2
    }
    else if (oldThr > 0) // 使用阈值初始化容量
        newCap = oldThr;
    else {
        // 原容量和阈值都是非正数，使用默认值初始化
        newCap = DEFAULT_INITIAL_CAPACITY;
        newThr = (int)(DEFAULT_LOAD_FACTOR * DEFAULT_INITIAL_CAPACITY);
    }
    // 第一次创建Map会在这里初始化threshold
    if (newThr == 0) {
        // 初始化新阈值
        float ft = (float)newCap * loadFactor;
        newThr = (newCap < MAXIMUM_CAPACITY && ft < (float)MAXIMUM_CAPACITY ? (int)ft : Integer.MAX_VALUE);
    }
    threshold = newThr;

    // 新建底层数组对象
    Node<K, V>[] newTab = (Node<K, V>[])new Node[newCap];
    // 将新底层数组赋值给全局变量table
    table = newTab;
    // ...
}
```

### 哈希计算

JDK中HashMap的实现里并没有直接使用Object的native方法返回的`hashCode`作为最终的哈希值，而是进行了二次加工，使用key对应的hashCode与其hashCode右移16位的结果进行异或操作（将高16位与低16位进行异或的操作称之为扰动函数，目的是将高位的特征融入到低位之中，降低哈希冲突的概率）。

```Java
static final int hash(Object key) {
    int h;
    // 扰动函数，降低哈希碰撞几率，高半区与低半区做异或，混合原始哈希值的高位与低位，以此来加大低位的随机性，而且混合后的低位掺杂了高位的部分特征，这样高位信息也被变相保留下来，哈希桶越小，效果越明显
    return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
}
```

在JDK 1.8之前，HashMap采用拉链表的方法解决哈希冲突，即当计算出的hashCode对应的桶上已经存在元素，但两者key不相同时，会基于桶中已存在的元素拉出一条链表，将新元素链到已存在元素的前面，当查询中存在冲突的哈希桶时，会顺序遍历冲突链上的元素，同一key的判断逻辑为`(p.hash == hash && ((k = p.key) == key || (key != null && key.equals(k))))`，先判断hash值是否相同，再比较key的地址或值是否相同。

JDK 1.8及之后，HashMap引入了红黑树来解决哈希冲突问题。当哈希冲突元素非常多时，拉链表查询性能将变差，在JDK 1.8中，如果冲突链上的元素数量大于8，并且哈希桶数组的长度大于64时，会使用红黑树来解决哈希冲突，此时节点被封装成TreeNode而不是Node(TreeNode继承了Node)，使查询具备O(logn)的性能。

### 位运算

**确定哈希桶数组大小** 找到大于等于给定值的最小2的整数次幂。`tableSizeFor()`根据输入容量大小cap来计算最终哈希桶数组的容量大小，找到大于等于给定值cap的最小2的整数次幂。

```Java
/**
 * 计算数组大小，返回2的幂，即扩容后的table大小
 * 找到cap对应二级制中最高位的1，然后每次以2倍的步长（依次移位1、2、4、8、16）复制最高位1到后面所有低位，把最高位1后面的所有位全部置为1，最后进行+1，即完成进位
 */
static final int tableSizeFor(int cap) {
    int n = cap - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
}
```

“找到大于等于给定值的最小2的整数次幂”中，“最小”是为了尽可能少占用空间，即最小可用原则，“2的整数次幂”是为了提高计算与存储效率，使每个元素对应hash值能够准确落入哈希桶数组给定的范围区间内。确定数组下标采用的算法是`hash & (n - 1)`，n即为哈希数组的大小，由于其总是2的整数次幂，n-1的二进制形式永远都是“0\*1\*”的形式，即从最低位开始，连续出现多个1，该值与任何值进行&运算都会使结果落入指定区间[0, n-1]中的任意一个值，即落入给定的n个哈希桶中任意一个桶[^举个反例，当n=7，n-1对应的二进制为0110，任何值与0110进行&运算都会落入到第0、2、4、6个哈希桶中，而不是\[0, 6\]的区间范围内，少了1、3、5三个哈希桶，这导致存储空间利用率只有不到60%，同时也增加了哈希碰撞的几率]。
