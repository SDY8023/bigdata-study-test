package arithmetic;

/**
 * @Author:SDY
 * @Description:
 * @Date: 22:57 2022/2/20
 * @Modified By:
 **/
public class test1 {
    public static void main(String[] args) {
        int[] nums = {20,7,11,15,2};
        int target = 9;
        int[] result = new test1().twoSum(nums, target);
        for (int i=0 ; i < result.length; i++){
            System.out.println(result[i]);
        }

    }

    /**
     *
     * @param nums
     * @param target
     * 给定一个整数数组 nums 和一个整数目标值 target，请你在该数组中找出 和为目标值 target  的那 两个 整数，并返回它们的数组下标。
     *
     * 你可以假设每种输入只会对应一个答案。但是，数组中同一个元素在答案里不能重复出现。
     *
     * 你可以按任意顺序返回答案。
     *
     * @return
     */
    public int[] twoSum(int[] nums,int target){
        int result[] = new int[2];
        for(int start = 0;start < nums.length -1; start++){
            for(int j = start + 1;j < nums.length;j++){
                if(nums[j] + nums[start] == target){
                    result[0] = start;
                    result[1] = j;
                }
            }

        }
        return result;
    }
}
