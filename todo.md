* make linked lists crash-safe
  * remove length field
  * add crasher tests for them
* add xxx_owned APIs, so we won't have to use to_owned all the time
  * or just make the APIs take owned values
* rename collections to lists
* add missing typed APIs for collections
* add peek/pop/push to linked lists

## crash-safe linked lists

in order to make linked-lists crash-safe, the algorithm will be as follows:

insert:
 * check for existance of item, if it exists, it's already a member of the list
 * if it does not exist, go to list.tail. this element must exist.
 * start walking from list.tail over the next elements until we find a valid item
 * this is the true tail of the list. if curr->next is valid but missing, we consider
   curr to be true end as well.
 * make curr->next = new item's key
 * insert new item (prev pointing to curr)
 * set list.tail = new item, len+=1

   this is safe because if we crash at any point, the list is still valid, and
   the accounting will be fixed by the next insert (patching len and tail)

removal:
 * check if the element exists. if not, no-op
 * if the element is the only item in the list, remove the list, and then remove the item.
 * if the element is the first in the (non-empty) list:
   * point list.head to element->next, set len-=1
   * point the new first element.prev = INVALID
   * remove the element
 * if the element is the last in the (non-empty) list:
   * point list.tail to element->prev, set len-=1
   * point the new last element.next = INVALID
   * remove the element
 * if the element is a middle element:
   * point element->prev->next to element->next -- now the element will not be traversed by iteration
   * point element->next->prev to element->prev -- now the element is completely disconnected
   * set list.len -= 1 -- THIS IS NOT CRASH SAFE. better remove the len altogether
   * remove the element

XXX: we need to allow the list to exist and point to a missing item (mostly only for the first and
only item case)