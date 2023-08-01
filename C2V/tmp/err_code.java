
static int f(int n)
{
    if (n == 0)
        return 1;

    return n * f(n-1);
}

static int g(int n)
{
    if (n == 0)
        return 1;

    return n * g(n-1);
}
    