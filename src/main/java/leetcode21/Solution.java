package leetcode21;

/**
 * @author Mackchao.Sun
 * @description:
 * @date:2022/4/21
 **/
public class Solution {
    public static ListNode mergeTwoLists(ListNode l1, ListNode l2) {
        if(l1 == null) {
            return l2;
        }
        if(l2 == null) {
            return l1;
        }

        if(l1.val < l2.val) {
            l1.next = mergeTwoLists(l1.next, l2);
            return l1;
        } else {
            l2.next = mergeTwoLists(l1, l2.next);
            return l2;
        }
    }

    public static void main(String[] args) {
        ListNode l1 = new ListNode(1);
        l1.next = new ListNode(3);
        l1.next.next = new ListNode(5);
        ListNode l2 = new ListNode(2);
        l2.next = new ListNode(4);
        l2.next.next = new ListNode(7);

        ListNode l3 = Solution.mergeTwoLists(l1,l2);
        while(l3.val != 0){
            System.out.println(l3.val);
            l3 = l3.next;
        }
    }
}
